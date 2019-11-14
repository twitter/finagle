package com.twitter.finagle.mux.pushsession

import com.twitter.finagle.Mux.param.{CompressionPreferences, TurnOnTlsFn}
import com.twitter.finagle.Stack
import com.twitter.finagle.mux.transport.Netty4Framer
import com.twitter.finagle.netty4.ssl.client.Netty4ClientSslChannelInitializer.OnSslHandshakeComplete
import com.twitter.finagle.pushsession.{PushChannelHandle, PushChannelHandleProxy}
import com.twitter.io.{Buf, ByteReader}
import com.twitter.logging.Logger
import com.twitter.util.Try
import io.netty.channel.Channel
import java.util.concurrent.atomic.AtomicBoolean

/**
 * The API for how to manipulate the handle on negotiation.  Exposed for testing.
 */
private[finagle] trait NegotiatingHandle {
  def turnOnTls(onHandshakeComplete: Try[Unit] => Unit): Unit
  def turnOnCompression(format: String): Unit
  def turnOnDecompression(format: String): Unit
}

/**
 * The only purpose of the MuxChannelHandle is to thread through the ability to
 * add the OpportunisticTls support. This is currently specialized to Netty4 since
 * we use the Netty4 TLS implementation.
 */
private[finagle] class MuxChannelHandle(
  underlying: PushChannelHandle[ByteReader, Buf],
  ch: Channel,
  params: Stack.Params)
    extends PushChannelHandleProxy[ByteReader, Buf](underlying)
    with NegotiatingHandle {

  private[this] val tlsGuard = new AtomicBoolean(false)

  /**
   * Enable TLS support by adding the appropriate handlers into netty.
   *
   * @param onHandshakeComplete Takes a callback which is called when the tls
   * handshake is complete.
   */
  def turnOnTls(onHandshakeComplete: Try[Unit] => Unit): Unit = {
    if (tlsGuard.compareAndSet(false, true)) {
      val prms = params + OnSslHandshakeComplete(onHandshakeComplete)
      params[TurnOnTlsFn].fn(prms, ch.pipeline)
    } else {
      MuxChannelHandle.log.warning("Attempted to turn on TLS multiple times")
    }
  }

  /**
   * Enable compression support by adding the appropriate handlers into netty.
   *
   * @param format Takes the name of a compression format to be installed
   *
   * @note this compresses after we length-encode because the Netty compressors
   * assume that they will see a byte stream, not a stream of messages of bytes.
   * the intuition here is that the length encoder takes a discrete message that
   * can't be broken up and turns it into undifferentiated bytes that can be
   * streamed.
   */
  def turnOnCompression(format: String): Unit = {
    val setting = params[CompressionPreferences].compressionPreferences.compression
    setting.transformers.find(_.name == format) match {
      case Some(transformer) =>
        ch.pipeline.addBefore(
          Netty4Framer.FrameEncoder,
          "frameCompressor",
          transformer()
        )
      case None =>
        throw new IllegalArgumentException(
          s"$format was picked as the compression format but we"
            + "don't know how to compress with that")
    }
  }

  /**
   * Enable decompression support by adding the appropriate handlers into netty.
   *
   * @param format Takes the name of a compression format to be installed
   *
   * @note this decompresses before we length-decode because the Netty
   * decompressors assume that they will see a byte stream, not a stream of
   * messages of bytes.  the intuition here is that the length decoder takes
   * undifferentiated bytes that can be streamed and turns them into discrete
   * messages.
   */
  def turnOnDecompression(format: String): Unit = {
    val setting = params[CompressionPreferences].compressionPreferences.decompression
    setting.transformers.find(_.name == format) match {
      case Some(transformer) =>
        ch.pipeline.addBefore(
          Netty4Framer.FrameDecoder,
          "frameDecompressor",
          transformer()
        )
      case None =>
        throw new IllegalArgumentException(
          s"$format was picked as the decompression format but we"
            + "don't know how to decompress with that")
    }
  }

  /**
   * A specialized method for sending data without going through the serial executor
   *
   * This lives only to avoid the potential race during opportunistic TLS negotiation
   * where we need to send our headers unencrypted to the peer, but immediately after
   * reconfigure the Netty pipeline for TLS, eg before receiving any more data (which
   * would be the TLS handshake). If we use the standard `sendAndForget`, we bounce
   * the write through the serial executor and there is no way to guarantee that the
   * pipeline refactor happens immediately after the headers have been encoded and made
   * it to the channels outbound byte buffer.
   */
  def sendNowAndForget(buf: Buf): Unit = {
    ch.writeAndFlush(buf, ch.voidPromise())
  }
}

private object MuxChannelHandle {
  private val log = Logger.get
}
