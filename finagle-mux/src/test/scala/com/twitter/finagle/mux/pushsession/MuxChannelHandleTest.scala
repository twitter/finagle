package com.twitter.finagle.mux.pushsession

import com.twitter.finagle.Mux
import com.twitter.finagle.mux.transport.{Compression, CompressionLevel, Netty4Framer}
import com.twitter.finagle.pushsession.utils.MockChannelHandle
import com.twitter.io.{Buf, ByteReader}
import io.netty.channel.ChannelDuplexHandler
import io.netty.channel.embedded.EmbeddedChannel
import scala.jdk.CollectionConverters._
import org.scalatest.funsuite.AnyFunSuite

class MuxChannelHandleTest extends AnyFunSuite {
  private[this] val prefs = Compression.LocalPreferences(
    Compression.LocalSetting(
      CompressionLevel.Desired,
      Seq(Compression.lz4Compressor(highCompression = false))),
    Compression.LocalSetting(CompressionLevel.Desired, Seq(Compression.lz4Decompressor()))
  )

  test("Turning on compression installs the right handler") {
    val underlying = new MockChannelHandle[ByteReader, Buf]()
    val embedded = new EmbeddedChannel()
    embedded.pipeline.addLast(Netty4Framer.FrameEncoder, new ChannelDuplexHandler())
    val params = Mux.client.params + Mux.param.CompressionPreferences(prefs)
    val handle = new MuxChannelHandle(underlying, embedded, params)

    handle.turnOnCompression("lz4")
    val names = embedded.pipeline.names.asScala.init // we init because the last is a TailContext
    assert(names == Seq("frameCompressor", Netty4Framer.FrameEncoder))
  }

  test("Turning on compression barfs on unknown format") {
    val underlying = new MockChannelHandle[ByteReader, Buf]()
    val embedded = new EmbeddedChannel()
    val params = Mux.client.params
    val handle = new MuxChannelHandle(underlying, embedded, params)

    intercept[IllegalArgumentException] {
      handle.turnOnCompression("lz4")
    }
  }

  test("Turning on decompression installs the right handler") {
    val underlying = new MockChannelHandle[ByteReader, Buf]()
    val embedded = new EmbeddedChannel()
    embedded.pipeline.addLast(Netty4Framer.FrameDecoder, new ChannelDuplexHandler())
    val params = Mux.client.params + Mux.param.CompressionPreferences(prefs)
    val handle = new MuxChannelHandle(underlying, embedded, params)

    handle.turnOnDecompression("lz4")
    val names = embedded.pipeline.names.asScala.init // we init because the last is a TailContext
    assert(names == Seq("frameDecompressor", Netty4Framer.FrameDecoder))
  }

  test("Turning on decompression barfs on unknown format") {
    val underlying = new MockChannelHandle[ByteReader, Buf]()
    val embedded = new EmbeddedChannel()
    val params = Mux.client.params
    val handle = new MuxChannelHandle(underlying, embedded, params)

    intercept[IllegalArgumentException] {
      handle.turnOnDecompression("lz4")
    }
  }
}
