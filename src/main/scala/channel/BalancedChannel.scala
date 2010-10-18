package com.twitter.netty.channel

import java.net.SocketAddress
import org.jboss.netty.channel._
import com.twitter.netty.util._
import com.twitter.netty.util.Conversions._

// keep most of the functionality here actually, but have it invoked
// by the sink. (the sink is rather primitive, and deals mostly with
// channel lifecycle issues.

class BalancedChannel(
  factory: BalancedChannelFactory,
  pipeline: ChannelPipeline,
  sink: ChannelSink)
  extends AbstractChannel(null/*parent*/, factory, pipeline, sink)
{
  val config = new DefaultChannelConfig
  var pool: Option[ChannelPool with SocketAddress] = None

  def connect(pool: ChannelPool with SocketAddress, future: ChannelFuture) {
    this.pool = Some(pool)
    future.setSuccess()
    Channels.fireChannelConnected(this, pool)
  }

  def close(future: ChannelFuture) {
    if (pool.isDefined) {
      // We're bound, so unbind / disconnect.
      Channels.fireChannelDisconnected(this)
      Channels.fireChannelUnbound(this)
    } else {
      pool = None
    }

    // Finally, close the channel.
    Channels.fireChannelClosed(this)
    future.setSuccess()
  }

  // TODO: local binding.
  def getRemoteAddress = pool.getOrElse(null)
  def getLocalAddress = getRemoteAddress

  // TODO: reflect real state.
  def isConnected = true
  def isBound = true
  def getConfig = config

  // This is where a lot of the tricky code lay: we need to handle
  // disconnections, application failures, etc. while making the pool
  // the arbiter of the policies regarding these events.
  object UpstreamHandler extends SimpleChannelUpstreamHandler {
    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      // Done with a request cycle: remove ourselves from the pipeline
      // & give the channel back to the pool.
      ctx.getChannel.getPipeline.remove(this)
      for (pool <- pool) pool.put(ctx.getChannel)
      Channels.fireMessageReceived(BalancedChannel.this, e.getMessage())
    }
  }

  def messageReceived(e: MessageEvent) {
    // TODO: buffer requests when another is outstanding.

    for (pool <- pool)
      pool.get() {
        case Ok(channel) =>
          channel.getPipeline.addLast("proxy", UpstreamHandler)
          Channels.write(channel, e.getMessage)
        case Error(_) =>
          // XXX - propagate the error.. need to ask the pool for its
          // retry policy. if we still can't make a request, then
          // we've failed, and we issue a disconnect / close.
          Channels.fireChannelDisconnected(this)
      }
  }
}