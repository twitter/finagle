package com.twitter.finagle.netty4.transport

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle._
import com.twitter.finagle.transport.Transport
import com.twitter.util._
import io.netty.channel.{Channel, ChannelFuture, ChannelFutureListener, ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.handler.ssl.SslHandler
import java.net.SocketAddress
import java.security.cert.Certificate
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.util.control.NonFatal

/**
 * A [[Transport]] implementation based on Netty's [[Channel]].
 *
 * Note: During the construction, a `ChannelTransport` inserts the terminating
 * inbound channel handler into the channel's pipeline so any inbound channel
 * handlers inserted after that won't get any of the inbound traffic.
 */
private[finagle] class ChannelTransport(ch: Channel) extends Transport[Any, Any] {

  import ChannelTransport._

  private[this] val queue = new AsyncQueue[Any]

  private[this] val failed = new AtomicBoolean(false)
  private[this] val closed = new Promise[Throwable]


  private[this] val readInterruptHandler: PartialFunction[Throwable, Unit] = {
    case e =>
      // technically we should decrement `msgsNeeded` here but since we fail
      // the transport on read interrupts, that becomes moot.
      fail(e)
  }

  val onClose: Future[Throwable] = closed

  private[transport] object ReadManager {
    // Negative `msgsNeeded` means we're buffering data in our offer queue
    // which hasn't been processed by the application yet, so rather than read
    // off the channel we drain the offer queue. This also applies back-pressure
    // at the tcp level. In order to proactively detect socket close events we
    // buffer up to 1 message because the TCP implementation only notifies the
    // channel of a close event when attempting to perform operations.
    private[this] val msgsNeeded = new AtomicInteger(0)

    // exposed for testing
    private[transport] def getMsgsNeeded = msgsNeeded.get

    // Tell the channel that we want to read if we don't have offers already queued
    def readIfNeeded(): Unit = {
      if (!ch.config.isAutoRead) {
        if (msgsNeeded.get >= 0) ch.read()
      }
    }

    // Increment our needed messages by 1 and ask the channel to read if necessary
    def incrementAndReadIfNeeded(): Unit = {
      val value = msgsNeeded.incrementAndGet()
      if (value >= 0 && !ch.config.isAutoRead) {
        ch.read()
      }
    }

    // Called when we have received a message from the channel pipeline
    def decrement(): Unit = {
      msgsNeeded.decrementAndGet()
    }
  }

  private[this] def fail(exc: Throwable): Unit = {
    if (!failed.compareAndSet(false, true))
      return

    // Do not discard existing queue items. Doing so causes a race
    // between reading off of the transport and a peer closing it.
    // For example, in HTTP, a remote server may send its content in
    // many chunks and then promptly close its connection.
    //
    // We do have to fail the queue before fail, otherwise control is
    // returned to netty potentially allowing subsequent offers to the queue,
    // which should be illegal after failure.
    queue.fail(exc, discard = false)

    // Note: we have to fail the queue before fail, otherwise control is
    // returned to netty potentially allowing subsequent offers to the queue,
    // which should be illegal after failure.
    close()
    closed.updateIfEmpty(Return(exc))
  }

  def write(msg: Any): Future[Unit] = {
    val op = ch.writeAndFlush(msg)

    val p = new Promise[Unit]
    op.addListener(new ChannelFutureListener {
      def operationComplete(f: ChannelFuture): Unit =
        if (f.isSuccess) p.setDone() else {
          p.setException(ChannelException(f.cause, remoteAddress))
        }
    })

    p
  }

  def read(): Future[Any] = {
    // We are going take one message from the queue so we increment our need by
    // one and potentially ask the channel to read based on the number of
    // buffered messages or pending read requests.
    ReadManager.incrementAndReadIfNeeded()

    // This is fine, but we should consider being a little more fine-grained
    // here. For example, if a read behind another read interrupts, perhaps the
    // transport shouldn't be failed, only the read dequeued.
    val p = new Promise[Any]

    // Note: We use `become` instead of `proxyTo` here even though `become` is
    // not recommended when `p` has interrupt handlers. `become` merges the
    // listeners of two promises, which continue to share state via Linked and
    // is a gain in space-efficiency.
    p.become(queue.poll())

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

  def peerCertificate: Option[Certificate] = ch.pipeline.get(classOf[SslHandler]) match {
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

  ch.pipeline.addLast(HandlerName, new ChannelInboundHandlerAdapter {

    override def channelActive(ctx: ChannelHandlerContext): Unit = {
      // Upon startup we immediately begin the process of buffering at most one inbound
      // message in order to detect channel close events. Otherwise we would have
      // different buffering behavior before and after the first `Transport.read()` event.
      ReadManager.readIfNeeded()
      super.channelActive(ctx)
    }

    override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
      // Check to see if we need more data
      ReadManager.readIfNeeded()
      super.channelReadComplete(ctx)
    }

    override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = {
      ReadManager.decrement()
      queue.offer(msg)
    }

    override def channelInactive(ctx: ChannelHandlerContext): Unit = {
      fail(new ChannelClosedException(remoteAddress))
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: Throwable): Unit = {
      fail(ChannelException(e, remoteAddress))
    }
  })
}

private[finagle] object ChannelTransport {
  val HandlerName: String = "finagleChannelTransport"
}
