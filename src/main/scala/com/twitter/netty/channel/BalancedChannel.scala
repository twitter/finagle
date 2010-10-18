package com.twitter.netty.channel

import org.jboss.netty.channel._
import com.twitter.netty.util._
import com.twitter.netty.util.Conversions._

// keep most of the functionality here actually, but have it invoked
// by the sink.

class BalancedChannel(
  factory: BalancedChannelFactory,
  pipeline: ChannelPipeline,
  sink: ChannelSink)
  extends AbstractChannel(null/* parent */, factory, pipeline, sink)
{
  val config = new DefaultChannelConfig
  @volatile private var balancedAddress: Option[BalancedAddress] = None

  protected[channel] def realConnect(balancedAddress: BalancedAddress, future: ChannelFuture) {
    this.balancedAddress = Some(balancedAddress)
    future.setSuccess()
    Channels.fireChannelConnected(this, balancedAddress)
  }

  protected[channel] def realClose(future: ChannelFuture) {
    if (balancedAddress.isDefined) {
      // We're bound, so unbind / disconnect.
      Channels.fireChannelDisconnected(this)
      Channels.fireChannelUnbound(this)
    } else {
      balancedAddress = None
    }

    Channels.fireChannelClosed(this)
    future.setSuccess()
  }

  val pool = null
  protected[channel] def realWrite(e: MessageEvent) {
    // TODO: buffer requests when another is outstanding.
    for (balancedAddress <- balancedAddress)
      balancedAddress.reserve() {
        case Ok(channel) =>
          channel.getPipeline.addLast("handleMessage", handleMessage)
          Channels.write(channel, e.getMessage)
        case Error(_) =>
          // XXX - propagate the error.. need to ask the pool for its
          // retry policy. if we still can't make a request, then
          // we've failed, and we issue a disconnect / realClose.
          Channels.fireChannelDisconnected(this)
      }
  }

  // TODO: local binding.
  def getRemoteAddress = balancedAddress.getOrElse(null)
  def getLocalAddress = getRemoteAddress

  // TODO: reflect real state.
  def isConnected = true
  def isBound = true
  def getConfig = config

  // This is where a lot of the tricky code lay: we need to handle
  // disconnections, application failures, etc. while making the pool
  // the arbiter of the policies regarding these events.
  private val handleMessage = new SimpleChannelUpstreamHandler {
    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      // Done with a request cycle: remove ourselves from the pipeline
      // & give the channel back to the pool.
      ctx.getChannel.getPipeline.remove(this)
      balancedAddress.foreach(_.release(ctx.getChannel))
      Channels.fireMessageReceived(BalancedChannel.this, e.getMessage)
    }
  }

}