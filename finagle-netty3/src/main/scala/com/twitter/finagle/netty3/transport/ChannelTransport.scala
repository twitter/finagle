package com.twitter.finagle.netty3.transport

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.transport.{Transport, TransportContext, LegacyContext}
import com.twitter.finagle.{ChannelClosedException, ChannelException, Status}
import com.twitter.util.{Future, Promise, Return, Time}
import java.net.SocketAddress
import java.security.cert.Certificate
import java.util.concurrent.atomic.AtomicBoolean
import org.jboss.netty.channel._
import org.jboss.netty.handler.ssl.SslHandler
import scala.util.control.NonFatal

class ChannelTransport[In, Out](ch: Channel)
    extends Transport[In, Out]
    with ChannelUpstreamHandler {
  type Context = TransportContext

  private[this] var nneed = 0
  private[this] def need(n: Int): Unit = synchronized {
    nneed += n
    // Note: we buffer 1 message here so that we receive socket
    // closes proactively.
    val r = nneed >= 0
    if (ch.isReadable != r && ch.isOpen)
      ch.setReadable(r)
  }

  ch.getPipeline.addLast("finagleTransportBridge", this)

  private[this] val readq = new AsyncQueue[Out]
  private[this] val failed = new AtomicBoolean(false)

  private[this] val readInterruptHandler: PartialFunction[Throwable, Unit] = {
    case e => fail(e)
  }

  private[this] def fail(exc: Throwable) {
    if (!failed.compareAndSet(false, true))
      return

    // Do not discard existing queue items. Doing so causes a race
    // between reading off of the transport and a peer closing it.
    // For example, in HTTP, a remote server may send its content in
    // many chunks and then promptly close its connection.
    readq.fail(exc, false)

    // Note: we have to fail the readq before fail, otherwise control is
    // returned to netty potentially allowing subsequent offers to the readq,
    // which should be illegal after failure.
    close()
    closep.updateIfEmpty(Return(exc))
  }

  override def handleUpstream(ctx: ChannelHandlerContext, e: ChannelEvent) {
    e match {
      case msg: MessageEvent =>
        readq.offer(msg.getMessage.asInstanceOf[Out])
        need(-1)

      case e: ChannelStateEvent
          if e.getState == ChannelState.OPEN && e.getValue != java.lang.Boolean.TRUE =>
        fail(new ChannelClosedException(ch.getRemoteAddress))

      case e: ChannelStateEvent if e.getState == ChannelState.INTEREST_OPS =>
        // Make sure we have the right interest ops. This allows us to fix
        // up any races that may occur when setting interest ops without
        // having to explicitly serialize them -- it guarantees convergence
        // of interest ops.
        //
        // This can't deadlock, because:
        //    #1 Updates from other threads are enqueued onto a pending
        //    operations queue for the owner thread, and they never wait
        //    for completion.
        //    #2 Within the context of this thread, Channel.isReadable cannot
        //    change while we're invoking setReadable(): subsequent channel
        //    state events will be terminated early by need()'s check.
        need(0)

      case e: ChannelStateEvent
          if e.getState == ChannelState.CONNECTED
            && e.getValue == java.lang.Boolean.TRUE =>
        need(0)

      case e: ExceptionEvent =>
        fail(ChannelException(e.getCause, ch.getRemoteAddress))

      case _ => // drop.
    }

    // We terminate the upstream here on purpose: this must always
    // be the last handler.
  }

  def write(msg: In): Future[Unit] = {
    val p = new Promise[Unit]

    // This is not cancellable because write operations in netty3
    // are note cancellable. That is, there is no way to interrupt or
    // preempt them once the write event has been sent into the pipeline.
    val writeFuture = new DefaultChannelFuture(ch, false /* cancellable */ )
    writeFuture.addListener(new ChannelFutureListener {
      def operationComplete(f: ChannelFuture): Unit = {
        if (f.isSuccess) p.setDone()
        else {
          // since we can't cancel, `f` must be an exception.
          p.setException(ChannelException(f.getCause, ch.getRemoteAddress))
        }
      }
    })

    // Ordering here is important. We want to call `addListener` on
    // `writeFuture` before giving it a chance to be satisfied, since
    // `addListener` will invoke all listeners on the calling thread
    // if the target future is complete. This allows us to present a
    // more consistent threading model where callbacks are invoked
    // on the event loop thread.
    ch.getPipeline().sendDownstream(new DownstreamMessageEvent(ch, writeFuture, msg, null))

    // We avoid setting an interrupt handler on the future exposed
    // because the backing opertion isn't interruptible.
    p
  }

  def read(): Future[Out] = {
    need(1)

    // This is fine, but we should consider being a little more fine-grained
    // here. For example, if a read behind another read interrupts, perhaps the
    // transport shouldn’t be failed, only the read dequeued.
    val p = new Promise[Out]

    // Note: We use become instead of proxyTo here even though become is
    // recommended when `p` has interrupt handlers. `become` merges the
    // listeners of two promises, which continue to share state via Linked and
    // is a gain in space-efficiency.
    p.become(readq.poll())

    // Note: We don't raise on readq.poll's future, because it doesn't set an
    // interrupt handler, but perhaps we should; and perhaps we should always
    // raise on the "other" side of the become indiscriminately in all cases.
    p.setInterruptHandler(readInterruptHandler)
    p
  }

  def status: Status =
    if (failed.get || !ch.isOpen) Status.Closed
    else Status.Open

  def close(deadline: Time): Future[Unit] = {
    if (ch.isOpen)
      Channels.close(ch)
    closep.unit
  }

  def localAddress: SocketAddress = ch.getLocalAddress()
  def remoteAddress: SocketAddress = ch.getRemoteAddress()

  val peerCertificate: Option[Certificate] =
    ch.getPipeline.get(classOf[SslHandler]) match {
      case null => None
      case handler =>
        try {
          handler.getEngine.getSession.getPeerCertificates.headOption
        } catch {
          case NonFatal(_) => None
        }
    }

  private[this] val closep = new Promise[Throwable]
  val onClose: Future[Throwable] = closep

  override def toString = s"Transport<channel=$ch, onClose=$closep>"

  val context: TransportContext = new LegacyContext(this)
}
