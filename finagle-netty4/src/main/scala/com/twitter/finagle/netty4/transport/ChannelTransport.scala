package com.twitter.finagle.netty4.transport

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle._
import com.twitter.finagle.transport.Transport
import com.twitter.util._
import io.netty.{channel => nchan}
import io.netty.handler.ssl.SslHandler
import java.net.SocketAddress
import java.security.cert.Certificate
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.util.control.NonFatal

/**
 * A [[Transport]] implementation based on Netty's [[nchan.Channel]].
 *
 * @param ch the underlying netty channel
 *
 * @param releaseMessage optional callback for releasing resources backing messages which
 *                       cannot be buffered in the offer queue.
 *
 * @param replacePending optional callback for releasing any resources behind enqueued
 *                       messages on transport shutdown / failure.
 *
 * @note During the construction, a `ChannelTransport` inserts the terminating
 *       inbound channel handler into the channel's pipeline so any inbound channel
 *       handlers inserted after that won't get any of the inbound traffic.
 *
 * @note This implementation can deterministically cleanup messages backed by external
 *       resources when the two optional functions `releaseMessage` and `replacePending`
 *       are defined.
 *
 *       There are three cases to consider:
 *
 *       1. A message is read from the underlying channel and subsequently
 *          read from the transport.
 *       2. A message is read from the underlying channel and never read from
 *          the transport.
 *       3. A message is read from the underlying channel and is not enqueued
 *          because the transport is shutting down or otherwise defunct.
 *
 *       In case #1, the only case where a satisfied read [[Future]] is produced,
 *       the onus is on the reading application to release any underlying direct
 *       buffers.
 *
 *       In the other cases this transport can release buffers as needed either when
 *       the session closes (case #2) or when an error state is detected (case #3).
 */
private[netty4] class ChannelTransport[In, Out](
    ch: nchan.Channel,
    releaseMessage: Out => Unit = { _: Out => () },
    replacePending: Out => Out = identity[Out]_)
  extends Transport[In, Out] {

  import ChannelTransport._



  // All access to `queue` needs to be mediated by `qLock` because we
  // need to fail + replace the pending elements atomically on shutdown.
  // With single-threaded netty4 events + transport reads we expect no contention
  // in the usual case.
  private[this] val qLock = new Object

  // Exposed as protected for testing
  protected[this] val queue = new AsyncQueue[Out]

  private[this] val failed = new AtomicBoolean(false)
  private[this] val closed = new Promise[Throwable]


  private[this] val readInterruptHandler: PartialFunction[Throwable, Unit] = {
    case e =>
      // Technically we should decrement `msgsNeeded` here but since we fail
      // the transport on read interrupts, that becomes moot.
      fail(e)
  }

  val onClose: Future[Throwable] = closed

  private[this] object ReadManager {
    // Negative `msgsNeeded` means we're buffering data in our offer queue
    // which hasn't been processed by the application yet, so rather than read
    // off the channel we drain the offer queue. This also applies back-pressure
    // at the tcp level. In order to proactively detect socket close events we
    // buffer up to 1 message because the TCP implementation only notifies the
    // channel of a close event when attempting to perform operations.
    private[this] val msgsNeeded = new AtomicInteger(0)

    // Tell the channel that we want to read if we don't have offers already queued
    def readIfNeeded(): Unit = {
      if (!ch.config.isAutoRead) {
        if (msgsNeeded.get >= 0) ch.read()
      }
    }

    // Increment our needed messages by 1 and ask the channel to read if necessary
    def incrementAndReadIfNeeded(): Unit = {
      if (!ch.config.isAutoRead) {
        if (msgsNeeded.incrementAndGet() >= 0) ch.read()
      }
    }

    // Called when we have received a message from the channel pipeline
    def decrementIfNeeded(): Unit = {
      if (!ch.config.isAutoRead) {
        msgsNeeded.decrementAndGet()
      }
    }
  }

  private[this] def fail(exc: Throwable): Unit = {
    if (!failed.compareAndSet(false, true))
      return

    // Replace buffered messages on shutdown / failure to support messages that need
    // clean up and then fail the queue atomically.
    qLock.synchronized {
      queue.drain() match {
        case Return(pending) =>
          pending.map(replacePending).foreach { queue.offer(_) }
        case _ =>
          ()
      }

      // We do have to fail the queue before fail exits, otherwise control is
      // returned to netty potentially allowing subsequent offers to the queue,
      // which should be illegal after failure. We also do not discard existing
      // queue items. Doing so causes a race between reading off of the transport
      // and a peer closing it. For example, in HTTP, a remote server may send its
      // content in many chunks and then promptly close its connection.
      queue.fail(exc, discard = false)
    }

    close()
    closed.updateIfEmpty(Return(exc))
  }

  def write(msg: In): Future[Unit] = {
    val op = ch.writeAndFlush(msg)

    val p = new Promise[Unit]
    op.addListener(new nchan.ChannelFutureListener {
      def operationComplete(f: nchan.ChannelFuture): Unit =
        if (f.isSuccess) p.setDone() else {
          p.setException(ChannelException(f.cause, remoteAddress))
        }
    })

    p
  }


  def read(): Future[Out] = {
    // We are going take one message from the queue so we increment our need by
    // one and potentially ask the channel to read based on the number of
    // buffered messages or pending read requests.
    ReadManager.incrementAndReadIfNeeded()

    // This is fine, but we should consider being a little more fine-grained
    // here. For example, if a read behind another read interrupts, perhaps the
    // transport shouldn't be failed, only the read dequeued.
    val p = new Promise[Out]

    // Note: We use `become` instead of `proxyTo` here even though `become` is
    // not recommended when `p` has interrupt handlers. `become` merges the
    // listeners of two promises, which continue to share state via Linked and
    // is a gain in space-efficiency.
    val el = qLock.synchronized {
      queue.poll()
    }
    p.become(el)


    // Note: We don't raise on queue.poll's future, because it doesn't set an
    // interrupt handler, but perhaps we should; and perhaps we should always
    // raise on the "other" side of the become indiscriminately in all cases.
    p.setInterruptHandler(readInterruptHandler)
    p
  }

  def status: Status =
    if (failed.get || !ch.isOpen) Status.Closed
    else if (!ch.isWritable) Status.Busy
    else Status.Open

  def close(deadline: Time): Future[Unit] = {
    if (ch.isOpen) ch.close()
    closed.unit
  }

  val peerCertificate: Option[Certificate] = ch.pipeline.get(classOf[SslHandler]) match {
    case null => None
    case handler =>
      try {
        handler.engine.getSession.getPeerCertificates.headOption
      } catch {
        case NonFatal(_) => None
      }
  }

  def localAddress: SocketAddress = ch.localAddress

  def remoteAddress: SocketAddress = ch.remoteAddress

  override def toString = s"Transport<channel=$ch, onClose=$closed>"

  ch.pipeline.addLast(HandlerName, new nchan.SimpleChannelInboundHandler[Out](false /* autoRelease */) {

    override def channelActive(ctx: nchan.ChannelHandlerContext): Unit = {
      // Upon startup we immediately begin the process of buffering at most one inbound
      // message in order to detect channel close events. Otherwise we would have
      // different buffering behavior before and after the first `Transport.read()` event.
      ReadManager.readIfNeeded()
      super.channelActive(ctx)
    }

    override def channelReadComplete(ctx: nchan.ChannelHandlerContext): Unit = {
      // Check to see if we need more data
      ReadManager.readIfNeeded()
      super.channelReadComplete(ctx)
    }

    override def channelRead0(ctx: nchan.ChannelHandlerContext, msg: Out): Unit = {
      ReadManager.decrementIfNeeded()

      // `offer` can fail in races between messages arriving and the transport shutting down
      // so we cleanup the underlying resources immediately. In practice for finagle protocols
      // we expect this lock to only ever see contention on read interrupts.
      val offerSucceeded = qLock.synchronized {
        queue.offer(msg)
      }

      if (!offerSucceeded) {
        releaseMessage(msg)

        // Dropped messages are fatal to the session
        fail(Failure("dropped read due to offer failure"))
      }
    }

    override def channelInactive(ctx: nchan.ChannelHandlerContext): Unit = {
      fail(new ChannelClosedException(remoteAddress))
    }

    override def exceptionCaught(ctx: nchan.ChannelHandlerContext, e: Throwable): Unit = {
      fail(ChannelException(e, remoteAddress))
    }
  })
}

private[finagle] object ChannelTransport {
  val HandlerName: String = "finagleChannelTransport"
}
