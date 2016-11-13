package com.twitter.finagle.http2.transport

import io.netty.handler.codec.http2._
import io.netty.handler.codec.http2.Http2HeadersEncoder.SensitivityDetector

// we need to supply no-op overrides for all of the methods to ensure the return
// types are correct.
private[http2] class RichHttpToHttp2ConnectionHandlerBuilder
  extends AbstractHttp2ConnectionHandlerBuilder[
    RichHttpToHttp2ConnectionHandler,
    RichHttpToHttp2ConnectionHandlerBuilder
  ] {

  override def validateHeaders(
    validateHeaders: Boolean
  ): RichHttpToHttp2ConnectionHandlerBuilder = {
    super.validateHeaders(validateHeaders)
  }

  override def initialSettings(settings: Http2Settings): RichHttpToHttp2ConnectionHandlerBuilder = {
    super.initialSettings(settings)
  }

  override def frameListener(
    frameListener: Http2FrameListener
  ): RichHttpToHttp2ConnectionHandlerBuilder = {
    super.frameListener(frameListener)
  }

  override def gracefulShutdownTimeoutMillis(
    gracefulShutdownTimeoutMillis: Long
  ): RichHttpToHttp2ConnectionHandlerBuilder = {
    super.gracefulShutdownTimeoutMillis(gracefulShutdownTimeoutMillis)
  }

  override def server(isServer: Boolean): RichHttpToHttp2ConnectionHandlerBuilder = {
    super.server(isServer)
  }

  override def connection(connection: Http2Connection): RichHttpToHttp2ConnectionHandlerBuilder = {
    super.connection(connection)
  }

  override def codec(
    decoder: Http2ConnectionDecoder,
    encoder: Http2ConnectionEncoder
  ): RichHttpToHttp2ConnectionHandlerBuilder = {
    super.codec(decoder, encoder)
  }

  override def frameLogger(
    frameLogger: Http2FrameLogger
  ): RichHttpToHttp2ConnectionHandlerBuilder = {
    super.frameLogger(frameLogger)
  }

  override def encoderEnforceMaxConcurrentStreams(
    encoderEnforceMaxConcurrentStreams: Boolean
  ): RichHttpToHttp2ConnectionHandlerBuilder = {
    super.encoderEnforceMaxConcurrentStreams(encoderEnforceMaxConcurrentStreams)
  }

  override def encoderIgnoreMaxHeaderListSize(
    ignoreMaxHeaderListSize: Boolean
  ): RichHttpToHttp2ConnectionHandlerBuilder = {
    super.encoderIgnoreMaxHeaderListSize(ignoreMaxHeaderListSize)
  }

  override def headerSensitivityDetector(
    headerSensitivityDetector: SensitivityDetector
  ): RichHttpToHttp2ConnectionHandlerBuilder = {
    super.headerSensitivityDetector(headerSensitivityDetector)
  }

  override def build(): RichHttpToHttp2ConnectionHandler = {
    super.build()
  }

  override protected def build(
    decoder: Http2ConnectionDecoder,
    encoder: Http2ConnectionEncoder,
    initialSettings: Http2Settings
  ): RichHttpToHttp2ConnectionHandler = {
    new RichHttpToHttp2ConnectionHandler(decoder, encoder, initialSettings)
  }
}
