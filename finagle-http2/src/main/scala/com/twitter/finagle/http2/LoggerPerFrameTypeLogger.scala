package com.twitter.finagle.http2

import com.twitter.logging.{Level, Logger}
import io.netty.buffer.{ByteBuf, ByteBufUtil}
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.http2.{Http2Flags, Http2FrameLogger, Http2Headers, Http2Settings}
import io.netty.handler.logging.LogLevel
import io.netty.util.internal.StringUtil

private[http2] class LoggerPerFrameTypeLogger(loggerNamePrefix: String)
    extends Http2FrameLogger(LogLevel.TRACE) {

  private[this] val prefix = loggerNamePrefix
  private[this] val rootFrameLogger = Logger.get(prefix)
  private[this] val dataLogger = Logger.get(prefix + ".DATA")
  private[this] val headersLogger = Logger.get(prefix + ".HEADERS")
  private[this] val priorityLogger = Logger.get(prefix + ".PRIORITY")
  private[this] val rstLogger = Logger.get(prefix + ".RST")
  private[this] val settingsLogger = Logger.get(prefix + ".SETTINGS")
  private[this] val pingLogger = Logger.get(prefix + ".PING")
  private[this] val pushPromiseLogger = Logger.get(prefix + ".PUSH_PROMISE")
  private[this] val goAwayLogger = Logger.get(prefix + ".GO_AWAY")
  private[this] val windowUpdateLogger = Logger.get(prefix + ".WINDOW_UPDATE")
  private[this] val unknownFrameLogger = Logger.get(prefix + ".UNKNOWN_FRAME")

  override def logData(
    direction: Http2FrameLogger.Direction,
    ctx: ChannelHandlerContext,
    streamId: Int,
    data: ByteBuf,
    padding: Int,
    endStream: Boolean
  ): Unit = {
    if (dataLogger.isLoggable(Level.TRACE)) {
      dataLogger.log(
        Level.TRACE,
        f"$direction ${ctx.channel} DATA: streamId=$streamId%d, padding=$padding%d, endStream=$endStream%b, length=${data.readableBytes}%d, bytes=${toString(dataLogger, data)}"
      )
    }
  }

  override def logHeaders(
    direction: Http2FrameLogger.Direction,
    ctx: ChannelHandlerContext,
    streamId: Int,
    headers: Http2Headers,
    padding: Int,
    endStream: Boolean
  ): Unit = {
    if (headersLogger.isLoggable(Level.TRACE)) {
      headersLogger.log(
        Level.TRACE,
        f"$direction ${ctx.channel} HEADERS: streamId=$streamId%d, headers=$headers, padding=$padding%d, endStream=$endStream%b"
      )
    }
  }

  override def logHeaders(
    direction: Http2FrameLogger.Direction,
    ctx: ChannelHandlerContext,
    streamId: Int,
    headers: Http2Headers,
    streamDependency: Int,
    weight: Short,
    exclusive: Boolean,
    padding: Int,
    endStream: Boolean
  ): Unit = {
    if (headersLogger.isLoggable(Level.TRACE)) {
      headersLogger.log(
        Level.TRACE,
        f"$direction ${ctx.channel} HEADERS: streamId=$streamId%d, headers=$headers, streamDependency=$streamDependency%d, weight=$weight%d, exclusive=$exclusive%b, padding=$padding%d, endStream=$endStream%b"
      )
    }
  }

  override def logPriority(
    direction: Http2FrameLogger.Direction,
    ctx: ChannelHandlerContext,
    streamId: Int,
    streamDependency: Int,
    weight: Short,
    exclusive: Boolean
  ): Unit = {
    if (priorityLogger.isLoggable(Level.TRACE)) {
      priorityLogger.log(
        Level.TRACE,
        f"$direction ${ctx.channel} PRIORITY: streamId=$streamId%d, streamDependency=$streamDependency%d, weight=$weight%d, exclusive=$exclusive%b"
      )
    }
  }

  override def logRstStream(
    direction: Http2FrameLogger.Direction,
    ctx: ChannelHandlerContext,
    streamId: Int,
    errorCode: Long
  ): Unit = {
    if (rstLogger.isLoggable(Level.TRACE)) {
      rstLogger.log(
        Level.TRACE,
        f"$direction ${ctx.channel} RST_STREAM: streamId=$streamId%d, errorCode=$errorCode%d"
      )
    }
  }

  override def logSettingsAck(
    direction: Http2FrameLogger.Direction,
    ctx: ChannelHandlerContext
  ): Unit = {
    if (settingsLogger.isLoggable(Level.TRACE)) {
      settingsLogger.log(Level.TRACE, s"$direction ${ctx.channel} SETTINGS: ack=true")
    }
  }

  override def logSettings(
    direction: Http2FrameLogger.Direction,
    ctx: ChannelHandlerContext,
    settings: Http2Settings
  ): Unit = {
    if (settingsLogger.isLoggable(Level.TRACE)) {
      settingsLogger.log(
        Level.TRACE,
        s"$direction ${ctx.channel} SETTINGS: ack=false, settings=$settings"
      )
    }
  }

  override def logPing(
    direction: Http2FrameLogger.Direction,
    ctx: ChannelHandlerContext,
    data: ByteBuf
  ): Unit = {
    if (pingLogger.isLoggable(Level.TRACE)) {
      pingLogger.log(
        Level.TRACE,
        f"$direction ${ctx.channel} PING: ack=false, length=${data.readableBytes}%d, bytes=${this
          .toString(pingLogger, data)}"
      )
    }
  }

  override def logPingAck(
    direction: Http2FrameLogger.Direction,
    ctx: ChannelHandlerContext,
    data: ByteBuf
  ): Unit = {
    if (pingLogger.isLoggable(Level.TRACE)) {
      pingLogger.log(
        Level.TRACE,
        f"$direction ${ctx.channel} PING: ack=true, length=${data.readableBytes}%d, bytes=${this
          .toString(pingLogger, data)}"
      )
    }
  }

  override def logPushPromise(
    direction: Http2FrameLogger.Direction,
    ctx: ChannelHandlerContext,
    streamId: Int,
    promisedStreamId: Int,
    headers: Http2Headers,
    padding: Int
  ): Unit = {
    if (pushPromiseLogger.isLoggable(Level.TRACE)) {
      pushPromiseLogger.log(
        Level.TRACE,
        f"$direction ${ctx.channel} PUSH_PROMISE: streamId=$streamId%d, promisedStreamId=$promisedStreamId%d, headers=$headers, padding=$padding%d"
      )
    }
  }

  override def logGoAway(
    direction: Http2FrameLogger.Direction,
    ctx: ChannelHandlerContext,
    lastStreamId: Int,
    errorCode: Long,
    debugData: ByteBuf
  ): Unit = {
    if (goAwayLogger.isLoggable(Level.TRACE)) {
      goAwayLogger.log(
        Level.TRACE,
        f"$direction ${ctx.channel} GO_AWAY: lastStreamId=$lastStreamId%d, errorCode=$errorCode%d, length=${debugData.readableBytes}%d, bytes=${this
          .toString(goAwayLogger, debugData)}"
      )
    }
  }

  override def logWindowsUpdate(
    direction: Http2FrameLogger.Direction,
    ctx: ChannelHandlerContext,
    streamId: Int,
    windowSizeIncrement: Int
  ): Unit = {
    if (windowUpdateLogger.isLoggable(Level.TRACE)) {
      windowUpdateLogger.log(
        Level.TRACE,
        f"$direction ${ctx.channel} WINDOW_UPDATE: streamId=$streamId%d, windowSizeIncrement=$windowSizeIncrement%d"
      )
    }
  }

  override def logUnknownFrame(
    direction: Http2FrameLogger.Direction,
    ctx: ChannelHandlerContext,
    frameType: Byte,
    streamId: Int,
    flags: Http2Flags,
    data: ByteBuf
  ): Unit = {
    if (unknownFrameLogger.isLoggable(Level.TRACE)) {
      unknownFrameLogger.log(
        Level.TRACE,
        f"$direction ${ctx.channel} UNKNOWN: frameType=${frameType & 255}%d, streamId=$streamId%d, flags=${flags.value}%d, length=${data.readableBytes}%d, bytes=${this
          .toString(unknownFrameLogger, data)}"
      )
    }
  }

  private[this] def toString(logger: Logger, buf: ByteBuf): String = {
    if (logger.isLoggable(Level.TRACE)) {
      ByteBufUtil.hexDump(buf)
    } else {
      StringUtil.EMPTY_STRING
    }
  }
}
