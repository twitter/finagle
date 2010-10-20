package com.twitter.finagle.channel

import java.nio.channels.NotYetConnectedException
import org.jboss.netty.channel.local.LocalAddress
import org.jboss.netty.channel._

class BrokeredChannel(
  factory: BrokeredChannelFactory,
  pipeline: ChannelPipeline,
  sink: ChannelSink)
  extends AbstractChannel(null/* parent */, factory, pipeline, sink)
{
  val config = new DefaultChannelConfig
  private val localAddress = new LocalAddress(LocalAddress.EPHEMERAL)
  @volatile private var broker: Option[Broker] = None

  protected[channel] def realConnect(broker: Broker, future: ChannelFuture) {
    this.broker = Some(broker)
    future.setSuccess()
    Channels.fireChannelConnected(this, broker)
    Channels.fireChannelBound(this, broker)
  }

  protected[channel] def realClose(future: ChannelFuture) {
    // TODO: if we have an outstanding request, notify the broker to
    // cancel requests (probably this means just sink them).
    if (broker.isDefined) {
      Channels.fireChannelDisconnected(this)
      Channels.fireChannelUnbound(this)
      broker = None
    }

    Channels.fireChannelClosed(this)
    future.setSuccess()
  }

  protected[channel] def realWrite(e: MessageEvent) {
    broker match {
      case Some(broker) => broker.dispatch(this, e)
      case None => e.getFuture.setFailure(new NotYetConnectedException)
    }
  }

  def getRemoteAddress = broker.getOrElse(null)
  def getLocalAddress = if (broker.isDefined) localAddress else null

  def isConnected = broker.isDefined
  def isBound = broker.isDefined
  def getConfig = config
}