package com.twitter.finagle.netty3.channel

/**
 * Dispatches channel events to a {{com.twitter.concurrent.Broker}}.
 */
import org.jboss.netty.channel._

import com.twitter.concurrent.{Broker, Offer}

class BrokerChannelHandler extends SimpleChannelHandler {
  sealed trait Event {
    type E <: ChannelEvent
    val e: E
    val ctx: ChannelHandlerContext

  }

  sealed trait UpstreamEvent extends Event {
    def sendUpstream(): Unit = {
      ctx.sendUpstream(e)
    }
  }

  case class Message(e: MessageEvent, ctx: ChannelHandlerContext) extends UpstreamEvent {
    type E = MessageEvent
  }
  case class WriteComplete(e: WriteCompletionEvent, ctx: ChannelHandlerContext)
      extends UpstreamEvent { type E = WriteCompletionEvent }
  case class ChildOpen(e: ChildChannelStateEvent, ctx: ChannelHandlerContext)
      extends UpstreamEvent { type E = ChildChannelStateEvent }
  case class ChildClosed(e: ChildChannelStateEvent, ctx: ChannelHandlerContext)
      extends UpstreamEvent { type E = ChildChannelStateEvent }
  case class Open(e: ChannelStateEvent, ctx: ChannelHandlerContext) extends UpstreamEvent {
    type E = ChannelStateEvent
  }
  case class Closed(e: ChannelStateEvent, ctx: ChannelHandlerContext) extends UpstreamEvent {
    type E = ChannelStateEvent
  }
  case class Bound(e: ChannelStateEvent, ctx: ChannelHandlerContext) extends UpstreamEvent {
    type E = ChannelStateEvent
  }
  case class Unbound(e: ChannelStateEvent, ctx: ChannelHandlerContext) extends UpstreamEvent {
    type E = ChannelStateEvent
  }
  case class Connected(e: ChannelStateEvent, ctx: ChannelHandlerContext) extends UpstreamEvent {
    type E = ChannelStateEvent
  }
  case class Disconnected(e: ChannelStateEvent, ctx: ChannelHandlerContext) extends UpstreamEvent {
    type E = ChannelStateEvent
  }
  case class InterestChanged(e: ChannelStateEvent, ctx: ChannelHandlerContext)
      extends UpstreamEvent { type E = ChannelStateEvent }
  case class Exception(e: ExceptionEvent, ctx: ChannelHandlerContext) extends UpstreamEvent {
    type E = ExceptionEvent
  }

  object MessageValue {
    def unapply(e: UpstreamEvent): Option[(Any, ChannelHandlerContext)] = e match {
      case Message(e, ctx) => Some((e.getMessage, ctx))
      case _ => None
    }
  }

  sealed trait DownstreamEvent extends Event {
    def sendDownstream(): Unit = {
      ctx.sendDownstream(e)
    }
  }

  case class Write(e: MessageEvent, ctx: ChannelHandlerContext) extends DownstreamEvent {
    type E = MessageEvent
  }
  case class Bind(e: ChannelStateEvent, ctx: ChannelHandlerContext) extends DownstreamEvent {
    type E = ChannelStateEvent
  }
  case class Connect(e: ChannelStateEvent, ctx: ChannelHandlerContext) extends DownstreamEvent {
    type E = ChannelStateEvent
  }
  case class InterestOps(e: ChannelStateEvent, ctx: ChannelHandlerContext) extends DownstreamEvent {
    type E = ChannelStateEvent
  }
  case class Disconnect(e: ChannelStateEvent, ctx: ChannelHandlerContext) extends DownstreamEvent {
    type E = ChannelStateEvent
  }
  case class Unbind(e: ChannelStateEvent, ctx: ChannelHandlerContext) extends DownstreamEvent {
    type E = ChannelStateEvent
  }
  case class Close(e: ChannelStateEvent, ctx: ChannelHandlerContext) extends DownstreamEvent {
    type E = ChannelStateEvent
  }

  object WriteValue {
    def unapply(e: DownstreamEvent): Option[(Any, ChannelHandlerContext)] = e match {
      case Write(e, ctx) => Some((e.getMessage, ctx))
      case _ => None
    }
  }

  /**
   * Proxy further downstream events.
   */
  protected def proxyDownstream(): Unit = {
    downstreamEvent foreach { _.sendDownstream() }
  }

  /**
   * Proxy further upstream events.
   */
  protected def proxyUpstream(): Unit = {
    upstreamEvent foreach { _.sendUpstream() }
  }

  /**
   * Proxy both upstream & downstream events.
   */
  protected def proxy(): Unit = {
    proxyUpstream()
    proxyDownstream()
  }

  val upstreamBroker = new Broker[UpstreamEvent]
  val upstreamEvent: Offer[UpstreamEvent] = upstreamBroker.recv

  val downstreamBroker = new Broker[DownstreamEvent]
  val downstreamEvent: Offer[DownstreamEvent] = downstreamBroker.recv

  /* Upstream */
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent): Unit = {
    upstreamBroker ! Message(e, ctx)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent): Unit = {
    // Exceptions are special: we always want to make sure we handle
    // them, so we're stricter: the receiver must synchronize immediately,
    // otherwise we proxy it upstream.
    //
    // This makes sure that exceptions always get propagated, even if
    // the channel handler process has died (eg. it threw an unhandled
    // exception).
    val of = upstreamBroker.send(Exception(e, ctx)) orElse Offer.const {
      super.exceptionCaught(ctx, e)
    }
    of.sync()
  }

  override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
    upstreamBroker ! Open(e, ctx)
  }

  override def channelBound(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
    upstreamBroker ! Bound(e, ctx)
  }

  override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
    upstreamBroker ! Connected(e, ctx)
  }

  override def channelInterestChanged(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
    upstreamBroker ! InterestChanged(e, ctx)
  }

  override def channelDisconnected(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
    upstreamBroker ! Disconnected(e, ctx)
  }

  override def channelUnbound(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
    upstreamBroker ! Unbound(e, ctx)
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
    upstreamBroker ! Closed(e, ctx)
  }

  override def writeComplete(ctx: ChannelHandlerContext, e: WriteCompletionEvent): Unit = {
    upstreamBroker ! WriteComplete(e, ctx)
  }

  override def childChannelOpen(ctx: ChannelHandlerContext, e: ChildChannelStateEvent): Unit = {
    upstreamBroker ! ChildOpen(e, ctx)
  }

  override def childChannelClosed(ctx: ChannelHandlerContext, e: ChildChannelStateEvent): Unit = {
    upstreamBroker ! ChildClosed(e, ctx)
  }

  /* Downstream */

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent): Unit = {
    downstreamBroker ! Write(e, ctx)
  }

  override def bindRequested(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
    downstreamBroker ! Bind(e, ctx)
  }

  override def connectRequested(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
    downstreamBroker ! Connect(e, ctx)
  }

  override def setInterestOpsRequested(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
    downstreamBroker ! InterestOps(e, ctx)
  }

  override def disconnectRequested(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
    downstreamBroker ! Disconnect(e, ctx)
  }

  override def unbindRequested(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
    downstreamBroker ! Unbind(e, ctx)
  }

  override def closeRequested(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
    downstreamBroker ! Close(e, ctx)
  }

}
