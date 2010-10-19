package com.twitter.finagle.channel

import org.jboss.netty.channel._

// keep most of the functionality here actually, but have it invoked
// by the sink.

class BrokeredChannel(
  factory: BrokeredChannelFactory,
  pipeline: ChannelPipeline,
  sink: ChannelSink)
  extends AbstractChannel(null/* parent */, factory, pipeline, sink)
{
  val config = new DefaultChannelConfig
  @volatile private var broker: Option[Broker] = None

  protected[channel] def realConnect(broker: Broker, future: ChannelFuture) {
    this.broker = Some(broker)
    future.setSuccess()
    Channels.fireChannelConnected(this, broker)
  }

  protected[channel] def realClose(future: ChannelFuture) {
    // to ensure consistency, we don't want to deliver any new
    // messages after the channel has been closed.

    // TODO: if we have an outstanding request, notify the broker to
    // cancel requests (probably this means just sink them).

    if (broker.isDefined) {
      Channels.fireChannelDisconnected(this)
      Channels.fireChannelUnbound(this)
    } else {
      broker = None
    }

    Channels.fireChannelClosed(this)
    future.setSuccess()
  }

  protected[channel] def realWrite(e: MessageEvent) {
    // XXX we lost the future here.
    broker.foreach(_.dispatch(this, e))
  }

  // TODO: local binding.
  def getRemoteAddress = broker.getOrElse(null)
  def getLocalAddress = getRemoteAddress // XXX

  // TODO: reflect real state.
  def isConnected = broker.isDefined
  def isBound = broker.isDefined
  def getConfig = config
}