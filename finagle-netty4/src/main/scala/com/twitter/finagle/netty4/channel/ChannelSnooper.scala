package com.twitter.finagle.netty4.channel

import io.netty.buffer.ByteBuf
import io.netty.channel._
import java.io.PrintStream
import java.net.SocketAddress
import java.nio.charset.Charset

/** Log events on channels */
private[netty4] trait ChannelSnooper extends ChannelDuplexHandler {
  def name: String

  private[this] lazy val printStream = new PrintStream(System.out, true, "UTF-8")

  def printer(message: String, exc: Throwable = null): Unit = {
    printStream.println(message)
    if (exc != null) {
      exc.printStackTrace(printStream)
    }
  }

  def print(id: ChannelId, indicator: String, message: String): Unit =
    printer(f"${id.asShortText()} $name%6s $indicator $message")

  def inboundIndicator = "(i)"
  def outboundIndicator = "(o)"
  def eventIndicator = "(e)"

  def printInbound(ch: Channel, message: String): Unit =
    print(ch.id, inboundIndicator, message)

  def printOutbound(ch: Channel, message: String): Unit =
    print(ch.id, outboundIndicator, message)

  // everything that's not an inbound/outbound message ie; flush, close, connect, etc
  def printEvent(ch: Channel, eventName: String): Unit =
    print(ch.id, eventIndicator, eventName)

}

/** Log message events */
private[netty4] class ByteBufSnooper(val name: String) extends ChannelSnooper {
  override val isSharable = true

  override def exceptionCaught(ctx: ChannelHandlerContext, exn: Throwable): Unit = {
    printer("Snooped exception", exn)
    super.exceptionCaught(ctx, exn)
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
    msg match {
      case buf: ByteBuf => dump(printInbound, ctx.channel, buf)
      case _ =>
    }

    super.channelRead(ctx, msg)
  }

  override def write(ctx: ChannelHandlerContext, msg: Object, promise: ChannelPromise): Unit = {
    msg match {
      case buf: ByteBuf => dump(printOutbound, ctx.channel, buf)
      case _ =>
    }

    super.write(ctx, msg, promise)
  }

  /**
   * print decoded channel messages
   */
  def dump(printer: (Channel, String) => Unit, ch: Channel, buf: ByteBuf): Unit = {
    val rawStr = buf.toString(buf.readerIndex, buf.readableBytes, Charset.forName("UTF-8"))
    val str = rawStr.replaceAll("\r", "\\\\r").replaceAll("\n", "\\\\n")
    val asciiStr = str.map { c => if (c >= 32 && c < 128) c else '?' }

    for (i <- 0 until asciiStr.length by 60)
      printer(ch, asciiStr.slice(i, i + 60).lines.mkString("\\n"))
  }
}

/** Log raw channel events without decoding channel messages */
private[netty4] class SimpleChannelSnooper(val name: String) extends ChannelSnooper {

  // outbound events
  override def write(ctx: ChannelHandlerContext, msg: Object, promise: ChannelPromise): Unit = {
    printOutbound(ctx.channel, s"WRITE ${msg.toString} to ${ctx.channel.remoteAddress}")
    super.write(ctx, msg, promise)
  }

  override def disconnect(ctx: ChannelHandlerContext, future: ChannelPromise): Unit = {
    printEvent(ctx.channel, "disconnect")
    super.disconnect(ctx, future)
  }

  override def flush(ctx: ChannelHandlerContext): Unit = {
    printEvent(ctx.channel, "flush")
    super.flush(ctx)
  }

  override def close(ctx: ChannelHandlerContext, future: ChannelPromise): Unit = {
    printEvent(ctx.channel, "close")
    super.close(ctx, future)
  }

  override def deregister(ctx: ChannelHandlerContext, future: ChannelPromise): Unit = {
    printEvent(ctx.channel, "deregister")
    super.deregister(ctx, future)
  }

  override def read(ctx: ChannelHandlerContext): Unit = {
    printEvent(ctx.channel, "read")
    super.read(ctx)
  }

  override def connect(
    ctx: ChannelHandlerContext,
    remoteAddress: SocketAddress,
    localAddress: SocketAddress,
    future: ChannelPromise
  ): Unit = {
    printEvent(ctx.channel, "connected to " + remoteAddress)
    super.connect(ctx, remoteAddress, localAddress, future)
  }

  override def bind(ctx: ChannelHandlerContext, localAddress: SocketAddress, future: ChannelPromise): Unit = {
    printEvent(ctx.channel, "bound to " + localAddress)
    super.bind(ctx, localAddress, future)
  }

  // inbound events
  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    printEvent(ctx.channel, "channel active")
    super.channelActive(ctx)
  }

  override def channelUnregistered(ctx: ChannelHandlerContext): Unit = {
    printEvent(ctx.channel, "channel unregistered")
    super.channelUnregistered(ctx)
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    printEvent(ctx.channel, "channel inactive")
    super.channelInactive(ctx)
  }

  override def channelWritabilityChanged(ctx: ChannelHandlerContext): Unit = {
    printEvent(ctx.channel, "channel writability changed")
    super.channelWritabilityChanged(ctx)
  }

  override def userEventTriggered(ctx: ChannelHandlerContext, evt: scala.Any): Unit = {
    printEvent(ctx.channel, "user event triggered")
    super.userEventTriggered(ctx, evt)
  }

  override def channelRegistered(ctx: ChannelHandlerContext): Unit = {
    printEvent(ctx.channel, "channel registered")
    super.channelRegistered(ctx)
  }

  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
    printEvent(ctx.channel, "channel read complete")
    super.channelReadComplete(ctx)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    printer("Snooped exception", cause)
    super.exceptionCaught(ctx, cause)
  }


  override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
    printInbound(ctx.channel, s"READ ${msg.toString} from ${ctx.channel.remoteAddress}")
    super.channelRead(ctx, msg)
  }
}

private[netty4] object ChannelSnooper {
  def apply(name: String)(thePrinter: (String, Throwable) => Unit): ChannelSnooper = {
    new SimpleChannelSnooper(name) {
      override def printer(message: String, exc: Throwable = null): Unit =
        thePrinter(message, exc)
    }
  }

  def addLast(name: String, p: ChannelPipeline): Unit =
    p.addLast(s"snooper-$name", new SimpleChannelSnooper(name))
}
