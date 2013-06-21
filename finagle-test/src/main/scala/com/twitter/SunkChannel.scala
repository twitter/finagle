package com.twitter.finagle

import scala.collection.mutable.ListBuffer

import org.jboss.netty.channel._

class SunkChannelSink extends AbstractChannelSink {
  val events = new ListBuffer[ChannelEvent]

  def eventSunk(p: ChannelPipeline, e: ChannelEvent) {
    events += e
  }
}

class SunkChannelFactory extends ChannelFactory {
  val sink = new SunkChannelSink

  def newChannel(pipeline: ChannelPipeline): Channel =
    new SunkChannel(this, pipeline, sink)

  def releaseExternalResources() = ()
  def shutdown() = ()
}

class SunkChannel(
  factory: SunkChannelFactory,
  pipeline: ChannelPipeline,
  sink: SunkChannelSink)
extends AbstractChannel(null/*parent*/, null/*factory*/, pipeline, sink)
{
  def upstreamEvents: Seq[ChannelEvent] = _upstreamEvents
  def downstreamEvents: Seq[ChannelEvent] = sink.events

  val _upstreamEvents = new ListBuffer[ChannelEvent]

  pipeline.addLast("upstreamSink", new ChannelUpstreamHandler {
    def handleUpstream(ctx: ChannelHandlerContext, e: ChannelEvent) =
      _upstreamEvents += e
  })

  var _isConnected = true
  var _isBound = true

  val config = new DefaultChannelConfig

  def getRemoteAddress = null
  def getLocalAddress = null

  def isConnected = _isConnected
  def isBound = _isBound

  def isConnected_=(yesno: Boolean) { _isConnected = yesno }
  def isBound_=(yesno: Boolean) { _isBound = yesno }

  def getConfig = config
}

object SunkChannel {
  def apply(p: ChannelPipeline) = new SunkChannelFactory().newChannel(p).asInstanceOf[SunkChannel]
}
