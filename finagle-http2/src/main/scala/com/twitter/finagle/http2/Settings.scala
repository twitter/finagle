package com.twitter.finagle.http2

import com.twitter.finagle.Stack
import com.twitter.finagle.http2.param._
import io.netty.handler.codec.http2.Http2Settings

private[http2] object Settings {
  private[http2] def fromParams(params: Stack.Params, isServer: Boolean): Http2Settings = {
    val HeaderTableSize(headerTableSize) = params[HeaderTableSize]
    val PushEnabled(pushEnabled) = params[PushEnabled]
    val MaxConcurrentStreams(maxConcurrentStreams) = params[MaxConcurrentStreams]
    val InitialWindowSize(initialWindowSize) = params[InitialWindowSize]
    val MaxFrameSize(maxFrameSize) = params[MaxFrameSize]
    val MaxHeaderListSize(maxHeaderListSize) = params[MaxHeaderListSize]

    val settings = new Http2Settings()
    headerTableSize.foreach { s => settings.headerTableSize(s.inBytes) }
    // this is a client-only parameter
    if (!isServer) {
      pushEnabled.foreach { settings.pushEnabled }
    }
    maxConcurrentStreams.foreach { settings.maxConcurrentStreams }
    initialWindowSize.foreach { s => settings.initialWindowSize(s.inBytes.toInt) }
    maxFrameSize.foreach { s => settings.maxFrameSize(s.inBytes.toInt) }
    settings.maxHeaderListSize(maxHeaderListSize.inBytes)
    settings
  }
}
