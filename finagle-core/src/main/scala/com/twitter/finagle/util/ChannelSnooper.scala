package com.twitter.finagle.util

import java.io.PrintStream
import java.nio.charset.Charset

import org.jboss.netty.channel._
import org.jboss.netty.buffer.ChannelBuffer

trait ChannelSnooper extends ChannelDownstreamHandler with ChannelUpstreamHandler {
  val name: String

  def printer(message: String) {
    (new PrintStream(System.out, true, "UTF-8")).println(message)
  }

  def print(indicator: String, message: String) {
    printer("%10s %s %s".format(name, indicator, message))
  }

  val upIndicator = "↑"
  val downIndicator = "↓"

  def printUp(message: String)   = print(upIndicator, message)
  def printDown(message: String) = print(downIndicator, message)
}

class ChannelBufferSnooper(val name: String) extends ChannelSnooper {
  // TODO: provide hexdump also. for now we deal only in ASCII
  // characters.

  override def handleUpstream(ctx: ChannelHandlerContext, e: ChannelEvent) {
    e match {
      case me: UpstreamMessageEvent if me.getMessage.isInstanceOf[ChannelBuffer] =>
        val buf = me.getMessage.asInstanceOf[ChannelBuffer]
        dump(printUp, buf)
      case _ =>
        ()
    }

    ctx.sendUpstream(e)
  }

  override def handleDownstream(ctx: ChannelHandlerContext, e: ChannelEvent) {
    e match {
      case me: DownstreamMessageEvent if me.getMessage.isInstanceOf[ChannelBuffer] =>
        val buf = me.getMessage.asInstanceOf[ChannelBuffer]
        dump(printDown, buf)
      case _ =>
        ()
    }

    ctx.sendDownstream(e)
  }

  // 67 characters
  def dump(printer: String => Unit, buf: ChannelBuffer) {
    val rawStr = buf.toString(buf.readerIndex, buf.readableBytes, Charset.forName("UTF-8"))
    val str = rawStr.replaceAll("\r", "\\\\r").replaceAll("\n", "\\\\n")

    for (i <- 0 until str.length by 60)
      printer(str.slice(i, i + 60).lines.mkString("\\n"))
  }
}

class SimpleChannelSnooper(val name: String) extends ChannelSnooper {
  override val upIndicator = "^"
  override val downIndicator = "v"

  override def handleUpstream(ctx: ChannelHandlerContext, e: ChannelEvent) {
    printUp(e.toString)
    if (e.isInstanceOf[ExceptionEvent])
      e.asInstanceOf[ExceptionEvent].getCause.printStackTrace()

    ctx.sendUpstream(e)
  }

  override def handleDownstream(ctx: ChannelHandlerContext, e: ChannelEvent) {
    printDown(e.toString)
    ctx.sendDownstream(e)
  }
}

object ChannelSnooper {
  def apply(name: String)(thePrinter: String => Unit) =
    new SimpleChannelSnooper(name) {
      override def printer(message: String) = thePrinter(message)
    }

  def addLast(name: String, p: ChannelPipeline) =
    p.addLast("snooper-%s".format(name), new SimpleChannelSnooper(name))

  def addFirst(name: String, p: ChannelPipeline) =
    p.addFirst("snooper-%s".format(name), new SimpleChannelSnooper(name))
}
