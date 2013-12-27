package com.twitter.finagle.transport

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.util.Proc
import com.twitter.finagle.{
  CancelledWriteException, ChannelClosedException, ChannelException}
import com.twitter.util.{Future, Return, Promise, Time}
import java.net.SocketAddress
import org.jboss.netty.channel._
import java.util.concurrent.atomic.AtomicBoolean

import com.twitter.finagle.stats.StatsReceiver

class ChannelTransport(ch: Channel)
  extends Transport[Any, Any] with ChannelUpstreamHandler
{
  ch.getPipeline.addLast("finagleTransportBridge", this)

  private[this] val readq = new AsyncQueue[Any]
  private[this] val writer = Proc[(Any, Promise[Unit])] { case (msg, p) =>
    Channels.write(ch, msg).addListener(new ChannelFutureListener {
      def operationComplete(f: ChannelFuture) {
        if (f.isSuccess)
          p.setDone()
        else if (f.isCancelled)
          p.setException(new CancelledWriteException)
        else
          p.setException(ChannelException(f.getCause, ch.getRemoteAddress))
      }
    })
  }
  private[this] val failed = new AtomicBoolean(false)

  private[this] def fail(exc: Throwable) {
    if (! failed.compareAndSet(false, true))
      return

    close()
    closep.updateIfEmpty(Return(exc))
    readq.fail(exc)
  }

  override def handleUpstream(ctx: ChannelHandlerContext, e: ChannelEvent) {
    e match {
      case msg: MessageEvent =>
        readq.offer(msg.getMessage.asInstanceOf[Any])

      case e: ChannelStateEvent
      if e.getState == ChannelState.OPEN && e.getValue != java.lang.Boolean.TRUE =>
        fail(new ChannelClosedException(ch.getRemoteAddress))

      case e: ExceptionEvent =>
        fail(ChannelException(e.getCause, ch.getRemoteAddress))

      case _ =>  // drop.
    }

    // We terminate the upstream here on purpose: this must always
    // be the last handler.
  }

  def write(msg: Any): Future[Unit] = {
    val p = new Promise[Unit]
    writer ! (msg, p)
    p
  }

  def read(): Future[Any] = readq.poll()

  def isOpen = ch.isOpen

  def close(deadline: Time) = {
    if (ch.isOpen)
      Channels.close(ch)

    closep map { _ => () }
  }

  def localAddress: SocketAddress = ch.getLocalAddress()
  def remoteAddress: SocketAddress = ch.getRemoteAddress()

  private[this] val closep = new Promise[Throwable]
  val onClose: Future[Throwable] = closep

  override def toString = "Transport<%s>".format(ch)
}


/**
 * Implements a {{Transport}} based on a Netty channel. It is a
 * {{ChannelHandler}} and must be the last in the pipeline.
 */
class ClientChannelTransport(ch: Channel, statsReceiver: StatsReceiver)
  extends Transport[Any, Any] with ChannelUpstreamHandler
{
  ch.getPipeline.addLast("finagleTransportBridge", this)

  private[this] val concurrentRequestCounter = 
    statsReceiver.counter("concurrent_request")
  private[this] val orphanResponseCounter = 
    statsReceiver.counter("orphan_response")
  private[this] val pending = new AtomicBoolean(false)

  private[this] val readq = new AsyncQueue[Any]
  private[this] val writer = Proc[(Any, Promise[Unit])] { case (msg, p) =>
    if (!pending.compareAndSet(false, true)) {
      concurrentRequestCounter.incr()
      p.setException(new Exception("write while request pending"))
    } else {
      Channels.write(ch, msg).addListener(new ChannelFutureListener {
        def operationComplete(f: ChannelFuture) {
          if (f.isSuccess)
            p.setDone()
          else if (f.isCancelled)
            p.setException(new CancelledWriteException)
          else
            p.setException(ChannelException(f.getCause, ch.getRemoteAddress))
        }
      })
    }
  }
  private[this] val failed = new AtomicBoolean(false)

  private[this] def fail(exc: Throwable) {
    if (! failed.compareAndSet(false, true))
      return

    close()
    closep.updateIfEmpty(Return(exc))
    readq.fail(exc)
  }

  override def handleUpstream(ctx: ChannelHandlerContext, e: ChannelEvent) {
    e match {
      case msg: MessageEvent =>
        if (!pending.compareAndSet(true, false)) {
          orphanResponseCounter.incr()
          close()
        } else {
          readq.offer(msg.getMessage.asInstanceOf[Any])
        }

      case e: ChannelStateEvent
      if e.getState == ChannelState.OPEN && e.getValue != java.lang.Boolean.TRUE =>
        fail(new ChannelClosedException(ch.getRemoteAddress))

      case e: ExceptionEvent =>
        fail(ChannelException(e.getCause, ch.getRemoteAddress))

      case _ => ()
    }

    // We terminate the upstream here on purpose: this must always
    // be the last handler.
  }

  def write(msg: Any): Future[Unit] = {
    val p = new Promise[Unit]
    writer ! (msg, p)
    p
  }

  def read(): Future[Any] = readq.poll()

  def isOpen = ch.isOpen

  def close(deadline: Time) = {
    if (ch.isOpen)
      Channels.close(ch)
    closep map { _ => () }
  }

  def localAddress: SocketAddress = ch.getLocalAddress()
  def remoteAddress: SocketAddress = ch.getRemoteAddress()

  private[this] val closep = new Promise[Throwable]
  val onClose: Future[Throwable] = closep

  override def toString = "Transport<%s>".format(ch)
}
