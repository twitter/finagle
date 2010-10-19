package com.twitter.finagle.util

import java.io.PrintStream
import org.jboss.netty.channel._

class ChannelSnooper(name: String) extends ChannelDownstreamHandler with ChannelUpstreamHandler {
  val printer: String => Unit = {
    (new PrintStream(System.out, true, "UTF-8")).println _
  }

  override def handleUpstream(ctx: ChannelHandlerContext, e: ChannelEvent) {
    printer("%10s ? %s".format(name, e))
    ctx.sendUpstream(e)
  }

  override def handleDownstream(ctx: ChannelHandlerContext, e: ChannelEvent) {
    printer("%10s ? %s".format(name, e))
    ctx.sendDownstream(e)
  }
}

object ChannelSnooper {
  def addLast(name: String, p: ChannelPipeline) =
    p.addLast("snooper-%s".format(name), new ChannelSnooper(name))

  def addFirst(name: String, p: ChannelPipeline) =
    p.addFirst("snooper-%s".format(name), new ChannelSnooper(name))
}