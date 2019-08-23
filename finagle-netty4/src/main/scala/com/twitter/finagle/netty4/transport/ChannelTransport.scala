package com.twitter.finagle.netty4.transport

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.{ChannelClosedException, ChannelException, Failure, Status}
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Future, Promise, Return, Time}
import io.netty.{channel => nettyChan}
import java.net.SocketAddress
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.util.control.NoStackTrace

/**
 * A [[Transport]] implementation based on Netty's [[nettyChan.Channel]].
 *
 * @param ch the underlying netty channel
 *
 * @param readQueue the queue used to buffer inbound messages
 *
 * @note During the construction, a `ChannelTransport` inserts the terminating
 *       inbound channel handler into the channel's pipeline so any inbound channel
 *       handlers inserted after that won't get any of the inbound traffic.
 */
private[finagle] class ChannelTransport(
  ch: nettyChan.Channel,
  readQueue: AsyncQueue[Any] = new AsyncQueue[Any],
  omitStackTraceOnInactive: Boolean = false)
    extends Transport[Any, Any] {

  type Context = ChannelTransportContext

  import ChannelTransport._

  // Accessible for testing
  private[transport] val closed = new Promise[Throwable]
  private[transport] val failed = new AtomicBoolean(false)

  private[this] val readInterruptHandler: PartialFunction[Throwable, Unit] = {
    case e =>
      // Technically we should decrement `msgsNeeded` here but since we fail
      // the transport on read interrupts, that becomes moot.
      fail(e)
  }

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

    // We do have to fail the queue before fail exits, otherwise control is
    // returned to netty potentially allowing subsequent offers to the queue,
    // which should be illegal after failure. We also do not discard existing
    // queue items. Doing so causes a race between reading off of the transport
    // and a peer closing it. For example, in HTTP, a remote server may send its
    // content in many chunks and then promptly close its connection.
    readQueue.fail(exc, discard = false)

    // Note: we have to fail the queue before fail, otherwise control is
    // returned to netty potentially allowing subsequent offers to the queue,
    // which should be illegal after failure.
    close()
    closed.updateIfEmpty(Return(exc))
  }

  def write(msg: Any): Future[Unit] = {
    val op = ch.writeAndFlush(msg)

    val p = new Promise[Unit]
    op.addListener(new nettyChan.ChannelFutureListener {
      def operationComplete(f: nettyChan.ChannelFuture): Unit =
        if (f.isSuccess) p.setDone()
        else {
          p.setException(ChannelException(f.cause, context.remoteAddress))
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
    p.become(readQueue.poll())

    // Note: We don't raise on queue.poll's future, because it doesn't set an
    // interrupt handler, but perhaps we should; and perhaps we should always
    // raise on the "other" side of the become indiscriminately in all cases.
    p.setInterruptHandler(readInterruptHandler)
    p
  }

  def status: Status =
    if (failed.get || !ch.isOpen) Status.Closed
    else Status.Open

  def onClose: Future[Throwable] = closed

  def close(deadline: Time): Future[Unit] = {
    if (ch.isOpen) ch.close()
    closed.unit
  }

  override def toString = s"Transport<channel=$ch, onClose=${closed}>"

  ch.pipeline.addLast(
    HandlerName,
    new nettyChan.ChannelInboundHandlerAdapter {

      override def channelActive(ctx: nettyChan.ChannelHandlerContext): Unit = {
        // Upon startup we immediately begin the process of buffering at most one inbound
        // message in order to detect channel close events. Otherwise we would have
        // different buffering behavior before and after the first `Transport.read()` event.
        ReadManager.readIfNeeded()
        super.channelActive(ctx)
      }

      override def channelReadComplete(ctx: nettyChan.ChannelHandlerContext): Unit = {
        // Check to see if we need more data
        ReadManager.readIfNeeded()
        super.channelReadComplete(ctx)
      }

      override def channelRead(ctx: nettyChan.ChannelHandlerContext, msg: Any): Unit = {
        ReadManager.decrement()

        if (!readQueue.offer(msg)) // Dropped messages are fatal
          fail(Failure(s"offer failure on $this $readQueue"))
      }

      override def channelInactive(ctx: nettyChan.ChannelHandlerContext): Unit = {
        if (omitStackTraceOnInactive) {
          fail(new NoStackTraceChannelClosedException(context.remoteAddress))
        } else fail(new ChannelClosedException(context.remoteAddress))
      }

      override def exceptionCaught(ctx: nettyChan.ChannelHandlerContext, e: Throwable): Unit = {
        fail(ChannelException(e, context.remoteAddress))
      }
    }
  )

  val context: ChannelTransportContext = new ChannelTransportContext(ch)
}

private[finagle] object ChannelTransport {
  val HandlerName: String = "finagleChannelTransport"

  private class NoStackTraceChannelClosedException(remoteAddress: SocketAddress)
      extends ChannelClosedException(remoteAddress)
      with NoStackTrace
}
