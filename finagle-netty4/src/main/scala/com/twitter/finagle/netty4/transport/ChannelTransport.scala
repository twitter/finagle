package com.twitter.finagle.netty4.transport

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle._
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Future, Promise, Return, Time}
import io.netty.channel.{
  Channel, ChannelHandlerContext, ChannelFutureListener, ChannelFuture,
  SimpleChannelInboundHandler
}
import java.net.SocketAddress
import java.security.cert.Certificate
import java.util.concurrent.atomic.AtomicBoolean

/**
 * A [[Transport]] implementation based on Netty's [[Channel]].
 *
 * Note: During the construction, a `ChannelTransport` inserts the terminating
 * inbound channel handler into the channel's pipeline so any inbound channel
 * handlers inserted after that won't get any of the inbound traffic.
 */
private[netty4] class ChannelTransport[In, Out](ch: Channel) extends Transport[In, Out] {
  private[this] val queue = new AsyncQueue[Out]

  private[this] val failed = new AtomicBoolean(false)
  private[this] val closed = new Promise[Throwable]

  private[this] val readInterruptHandler: PartialFunction[Throwable, Unit] = {
    case e => fail(e)
  }

  val onClose: Future[Throwable] = closed

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

  def write(msg: In): Future[Unit] = {
    // We support Netty's channel-level backpressure thereby respecting
    // slow receivers on the other side.
    if (!ch.isWritable) {
      // Note: It's up to the layer above a transport to decided whether or
      // not to requeue a canceled write.
      Future.exception(new DroppedWriteException)
    } else {
      val op = ch.writeAndFlush(msg)

      val p = new Promise[Unit]
      op.addListener(new ChannelFutureListener {
        def operationComplete(f: ChannelFuture): Unit =
          if (f.isSuccess)
            p.setDone()
          else if (f.isCancelled)
            p.setException(new CancelledWriteException)
          else
            p.setException(ChannelException(f.cause, remoteAddress))
      })

      p.setInterruptHandler { case _ => op.cancel(true /* mayInterruptIfRunning */) }
      p
    }
  }

  def read(): Future[Out] = {
    ch.read()

    // This is fine, but we should consider being a little more fine-grained
    // here. For example, if a read behind another read interrupts, perhaps the
    // transport shouldn't be failed, only the read dequeued.
    val p = new Promise[Out]

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

  // TODO: We don't support SSL for Netty 4 yet
  val peerCertificate: Option[Certificate] = None

  def localAddress: SocketAddress = ch.localAddress

  def remoteAddress: SocketAddress= ch.remoteAddress

  override def toString = s"Transport<channel=$ch, onClose=$closed>"

  ch.pipeline().addLast("finagleChannelTransport", new SimpleChannelInboundHandler[Out]() {
    override def channelRead0(ctx: ChannelHandlerContext, msg: Out): Unit = {
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
