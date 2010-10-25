package com.twitter.finagle.util

import java.io.PrintStream
import java.nio.charset.Charset

import org.jboss.netty.channel._
import org.jboss.netty.buffer.ChannelBuffer

trait ChannelSnooper extends ChannelDownstreamHandler with ChannelUpstreamHandler {
  val name: String

  val printer: String => Unit = {
    (new PrintStream(System.out, true, "UTF-8")).println _
  }

  def print(indicator: String, message: String) {
    printer("%10s %s %s".format(name, indicator, message))
  }

  def printUp(message: String)   = print("↑", message)
  def printDown(message: String) = print("↓", message)
}

class ChannelBufferSnooper(val name: String) extends ChannelSnooper {
  // TODO: provide hexdump also. for now we deal only in ASCII
  // characters.

  override def handleUpstream(ctx: ChannelHandlerContext, e: ChannelEvent) {
    e match {
      case me: UpstreamMessageEvent if me.isInstanceOf[ChannelBuffer] =>
        val buf = me.asInstanceOf[ChannelBuffer]
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
  override def handleUpstream(ctx: ChannelHandlerContext, e: ChannelEvent) {
    printUp(e.toString)
    ctx.sendUpstream(e)
  }

  override def handleDownstream(ctx: ChannelHandlerContext, e: ChannelEvent) {
    printDown(e.toString)
    ctx.sendDownstream(e)
  }
}

// One with byte.

object ChannelSnooper {
  def addLast(name: String, p: ChannelPipeline) =
    p.addLast("snooper-%s".format(name), new SimpleChannelSnooper(name))

  def addFirst(name: String, p: ChannelPipeline) =
    p.addFirst("snooper-%s".format(name), new SimpleChannelSnooper(name))
}
