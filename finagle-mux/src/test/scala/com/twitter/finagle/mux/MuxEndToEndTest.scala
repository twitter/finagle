package com.twitter.finagle.mux

import com.twitter.conversions.StorageUnitOps._
import com.twitter.finagle.Mux.param.MaxFrameSize
import com.twitter.finagle.mux.transport.{Compression, CompressionLevel}
import com.twitter.finagle.Mux

class EndToEndTest extends AbstractEndToEndTest {
  override type ClientT = Mux.Client
  override type ServerT = Mux.Server
  def implName: String = "push-based"
  def clientImpl() = Mux.client
  def serverImpl() = Mux.server
}

class FragmentingEndToEndTest extends AbstractEndToEndTest {
  override type ClientT = Mux.Client
  override type ServerT = Mux.Server
  def implName: String = "push-based"
  def clientImpl() = Mux.client.configured(MaxFrameSize(5.bytes))
  def serverImpl() = Mux.server.configured(MaxFrameSize(5.bytes))
}

abstract class CompressingEndToEndTest extends AbstractEndToEndTest {
  override type ClientT = Mux.Client
  override type ServerT = Mux.Server

  override def skipWholeTest: Boolean = sys.props.contains("SKIP_FLAKY_TRAVIS")

  val compressor = Seq(Compression.lz4Compressor(highCompression = true))
  val decompressor = Seq(Compression.lz4Decompressor())
}

class CompressionEnabledTest extends CompressingEndToEndTest {
  def implName: String = "compression-enabled"

  def clientImpl() =
    Mux.client.withCompressionPreferences
      .compression(CompressionLevel.Desired, compressor)
      .withCompressionPreferences.decompression(CompressionLevel.Desired, decompressor)
  def serverImpl() =
    Mux.server.withCompressionPreferences
      .compression(CompressionLevel.Desired, compressor)
      .withCompressionPreferences.decompression(CompressionLevel.Desired, decompressor)
}

class CompressionClientAcceptingTest extends CompressingEndToEndTest {
  def implName: String = "compression-client-enabled"

  def clientImpl() =
    Mux.client.withCompressionPreferences
      .compression(CompressionLevel.Accepted, compressor)
      .withCompressionPreferences.decompression(CompressionLevel.Accepted, decompressor)
  def serverImpl() =
    Mux.server.withCompressionPreferences
      .compression(CompressionLevel.Desired, compressor)
      .withCompressionPreferences.decompression(CompressionLevel.Desired, decompressor)
}

class CompressionServerAcceptingTest extends CompressingEndToEndTest {
  def implName: String = "compression-client-enabled"

  def clientImpl() =
    Mux.client.withCompressionPreferences
      .compression(CompressionLevel.Desired, compressor)
      .withCompressionPreferences.decompression(CompressionLevel.Desired, decompressor)
  def serverImpl() =
    Mux.server.withCompressionPreferences
      .compression(CompressionLevel.Accepted, compressor)
      .withCompressionPreferences.decompression(CompressionLevel.Accepted, decompressor)
}

class CompressionClientDisabledTest extends CompressingEndToEndTest {
  def implName: String = "compression-client-enabled"

  def clientImpl() = Mux.client
  def serverImpl() =
    Mux.server.withCompressionPreferences
      .compression(CompressionLevel.Desired, compressor)
      .withCompressionPreferences.decompression(CompressionLevel.Desired, decompressor)
}

class CompressionServerDisabledTest extends CompressingEndToEndTest {
  def implName: String = "compression-server-enabled"

  def clientImpl() =
    Mux.client.withCompressionPreferences
      .compression(CompressionLevel.Desired, compressor)
      .withCompressionPreferences.decompression(CompressionLevel.Desired, decompressor)
  def serverImpl() = Mux.server
}
