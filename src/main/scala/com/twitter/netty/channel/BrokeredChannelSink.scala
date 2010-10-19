package com.twitter.netty.channel

import org.jboss.netty.channel._

class BrokeredChannelSink extends AbstractChannelSink {
  override def eventSunk(p: ChannelPipeline, e: ChannelEvent) {
    e match {
      case e: ChannelStateEvent =>
        handleChannelStateEvent(p, e)
      case e: MessageEvent =>
        handleMessageEvent(p, e)
    }
  }

  def handleChannelStateEvent(p: ChannelPipeline, e: ChannelStateEvent) {
    val ch = e.getChannel.asInstanceOf[BrokeredChannel]
    val value = e.getValue
    val future = e.getFuture

    e.getState match {
      case ChannelState.OPEN =>
        if (java.lang.Boolean.FALSE eq value)
          ch.realClose(future)
      case ChannelState.BOUND =>
        // XXX - dispatch bound/connected, too?
        if (value ne null) {
          future.setSuccess()
          Channels.fireChannelBound(ch, value.asInstanceOf[BrokeredAddress])
        } else {
          ch.realClose(future)
        }
      case ChannelState.CONNECTED =>
        if (value ne null)
          ch.realConnect(value.asInstanceOf[BrokeredAddress], future)
        else
          ch.realClose(future)
      case ChannelState.INTEREST_OPS =>
        // TODO: not yet supported, but may be relevant to us.
        future.setSuccess()
    }
  }

  def handleMessageEvent(p: ChannelPipeline, e: MessageEvent) {
    val ch = e.getChannel.asInstanceOf[BrokeredChannel]
    ch.realWrite(e)
  }

}