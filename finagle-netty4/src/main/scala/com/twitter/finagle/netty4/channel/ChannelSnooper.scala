package com.twitter.finagle.netty4.channel

import io.netty.buffer.{ByteBuf, ByteBufUtil}
import io.netty.channel._
import io.netty.channel.ChannelHandler.Sharable
import java.io.PrintStream
import java.net.SocketAddress

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

  def inboundIndicator: String = "(i)"
  def outboundIndicator: String = "(o)"
  def eventIndicator: String = "(e)"

  def printInbound(ch: Channel, message: String): Unit =
    print(ch.id, inboundIndicator, message)

  def printOutbound(ch: Channel, message: String): Unit =
    print(ch.id, outboundIndicator, message)

  // everything that's not an inbound/outbound message ie; flush, close, connect, etc
  def printEvent(ch: Channel, eventName: String): Unit =
    print(ch.id, eventIndicator, eventName)

}

/** Log message events */
@Sharable
private[netty4] class ByteBufSnooper(val name: String) extends ChannelSnooper {

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
    printer(ch, ByteBufUtil.hexDump(buf))
  }
}

/** Log raw channel events without decoding channel messages */
@Sharable
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

  override def bind(
    ctx: ChannelHandlerContext,
    localAddress: SocketAddress,
    future: ChannelPromise
  ): Unit = {
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

private[finagle] object ChannelSnooper {

  /**
   * Makes a ChannelSnooper that will log however you want, printing out the
   * objects.
   *
   * @param thePrinter: Handles printing the snooped objects.  When not logging
   *        an exception, the `Throwable` argument will be null
   */
  def apply(name: String)(thePrinter: (String, Throwable) => Unit): ChannelSnooper = {
    new SimpleChannelSnooper(name) {
      override def printer(message: String, exc: Throwable = null): Unit =
        thePrinter(message, exc)
    }
  }

  /**
   * Makes a ChannelSnooper that will log however you want, printing out the
   * bytes in utf8.
   *
   * @param thePrinter: Handles printing the snooped bytes.  When not logging an
   *        exception, the `Throwable` argument will be null
   */
  def byteSnooper(name: String)(thePrinter: (String, Throwable) => Unit): ChannelSnooper = {
    new ByteBufSnooper(name) {
      override def printer(message: String, exc: Throwable = null): Unit =
        thePrinter(message, exc)
    }
  }

  def addLast(name: String, p: ChannelPipeline): Unit =
    p.addLast(s"snooper-$name", new SimpleChannelSnooper(name))
}
