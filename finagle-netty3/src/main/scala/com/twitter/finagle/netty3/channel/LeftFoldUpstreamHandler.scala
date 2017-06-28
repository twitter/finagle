package com.twitter.finagle.netty3.channel

/**
 * Introduces a "foldable" channel handler for easy state machine
 * management. For certain use cases, these both simplify state
 * machines and enhance composability.
 */

import org.jboss.netty.channel._

import com.twitter.concurrent.Serialized

class LeftFoldUpstreamHandler {
  def channelHandler = new LeftFoldHandlerToChannelHandler(this)

  def channelBound(
    ctx: ChannelHandlerContext,
    e: ChannelStateEvent
  ): LeftFoldUpstreamHandler = {
    ctx.sendUpstream(e)
    this
  }

  def channelClosed(
    ctx: ChannelHandlerContext,
    e: ChannelStateEvent
  ): LeftFoldUpstreamHandler = {
    ctx.sendUpstream(e)
    this
  }

  def channelConnected(
    ctx: ChannelHandlerContext,
    e: ChannelStateEvent
  ): LeftFoldUpstreamHandler = {
    ctx.sendUpstream(e)
    this
  }

  def channelDisconnected(
    ctx: ChannelHandlerContext,
    e: ChannelStateEvent
  ): LeftFoldUpstreamHandler = {
    ctx.sendUpstream(e)
    this
  }

  def channelInterestChanged(
    ctx: ChannelHandlerContext,
    e: ChannelStateEvent
  ): LeftFoldUpstreamHandler = {
    ctx.sendUpstream(e)
    this
  }

  def channelOpen(
    ctx: ChannelHandlerContext,
    e: ChannelStateEvent
  ): LeftFoldUpstreamHandler = {
    ctx.sendUpstream(e)
    this
  }

  def channelUnbound(
    ctx: ChannelHandlerContext,
    e: ChannelStateEvent
  ): LeftFoldUpstreamHandler = {
    ctx.sendUpstream(e)
    this
  }

  def childChannelClosed(
    ctx: ChannelHandlerContext,
    e: ChildChannelStateEvent
  ): LeftFoldUpstreamHandler = {
    ctx.sendUpstream(e)
    this
  }

  def childChannelOpen(
    ctx: ChannelHandlerContext,
    e: ChildChannelStateEvent
  ): LeftFoldUpstreamHandler = {
    ctx.sendUpstream(e)
    this
  }

  def exceptionCaught(
    ctx: ChannelHandlerContext,
    e: ExceptionEvent
  ): LeftFoldUpstreamHandler = {
    ctx.sendUpstream(e)
    this
  }

  def messageReceived(
    ctx: ChannelHandlerContext,
    e: MessageEvent
  ): LeftFoldUpstreamHandler = {
    ctx.sendUpstream(e)
    this
  }

  def writeComplete(
    ctx: ChannelHandlerContext,
    e: WriteCompletionEvent
  ): LeftFoldUpstreamHandler = {
    ctx.sendUpstream(e)
    this
  }
}

private[channel] class LeftFoldHandlerToChannelHandler(initial: LeftFoldUpstreamHandler)
  extends SimpleChannelUpstreamHandler with Serialized
{
  private[this] var state = initial

  override def handleUpstream(ctx: ChannelHandlerContext, e: ChannelEvent) =
    serialized { super.handleUpstream(ctx, e) }

  override def channelBound(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    state = state.channelBound(ctx, e)
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    state = state.channelClosed(ctx, e)
  }

  override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    state = state.channelConnected(ctx, e)
  }

  override def channelDisconnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    state = state.channelDisconnected(ctx, e)
  }

  override def channelInterestChanged(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    state = state.channelInterestChanged(ctx, e)
  }

  override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    state = state.channelOpen(ctx, e)
  }

  override def channelUnbound(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    state = state.channelUnbound(ctx, e)
  }

  override def childChannelClosed(ctx: ChannelHandlerContext, e: ChildChannelStateEvent) {
    state = state.childChannelClosed(ctx, e)
  }

  override def childChannelOpen(ctx: ChannelHandlerContext, e: ChildChannelStateEvent) {
    state = state.childChannelOpen(ctx, e)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    state = state.exceptionCaught(ctx, e)
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    state = state.messageReceived(ctx, e)
  }

  override def writeComplete(ctx: ChannelHandlerContext, e: WriteCompletionEvent) {
    state = state.writeComplete(ctx, e)
  }
}
