package com.twitter.finagle.mux

import com.twitter.conversions.StorageUnitOps._
import com.twitter.finagle.Mux.param.MaxFrameSize
import com.twitter.finagle.mux.transport.Compression
import com.twitter.finagle.mux.transport.CompressionLevel
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

  override def skipWholeTest: Boolean = sys.props.contains("SKIP_FLAKY_TRAVIS")

  def clientImpl() = Mux.client.configured(MaxFrameSize(5.bytes))
  def serverImpl() = Mux.server.configured(MaxFrameSize(5.bytes))
}

abstract class Lz4CompressingEndToEndTest extends AbstractEndToEndTest {
  override type ClientT = Mux.Client
  override type ServerT = Mux.Server

  override def skipWholeTest: Boolean = sys.props.contains("SKIP_FLAKY_TRAVIS")

  val compressor = Seq(Compression.lz4Compressor(highCompression = true))
  val decompressor = Seq(Compression.lz4Decompressor())
}

class Lz4CompressionEnabledTest extends Lz4CompressingEndToEndTest {
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

class Lz4CompressionClientAcceptingTest extends Lz4CompressingEndToEndTest {
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

class Lz4CompressionServerAcceptingTest extends Lz4CompressingEndToEndTest {
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

class Lz4CompressionClientDisabledTest extends Lz4CompressingEndToEndTest {
  def implName: String = "compression-client-enabled"

  def clientImpl() = Mux.client
  def serverImpl() =
    Mux.server.withCompressionPreferences
      .compression(CompressionLevel.Desired, compressor)
      .withCompressionPreferences.decompression(CompressionLevel.Desired, decompressor)
}

class Lz4CompressionServerDisabledTest extends Lz4CompressingEndToEndTest {
  def implName: String = "compression-server-enabled"

  def clientImpl() =
    Mux.client.withCompressionPreferences
      .compression(CompressionLevel.Desired, compressor)
      .withCompressionPreferences.decompression(CompressionLevel.Desired, decompressor)
  def serverImpl() = Mux.server
}

abstract class ZstdCompressingEndToEndTest extends AbstractEndToEndTest {
  override type ClientT = Mux.Client
  override type ServerT = Mux.Server

  override def skipWholeTest: Boolean = sys.props.contains("SKIP_FLAKY_TRAVIS")

  val compressor = Seq(Compression.zstdCompressor())
  val decompressor = Seq(Compression.zstdDecompressor())
}

class ZstdCompressionEnabledTest extends ZstdCompressingEndToEndTest {
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

class ZstdCompressionClientAcceptingTest extends ZstdCompressingEndToEndTest {
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

class ZstdCompressionServerAcceptingTest extends ZstdCompressingEndToEndTest {
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

class ZstdCompressionClientDisabledTest extends ZstdCompressingEndToEndTest {
  def implName: String = "compression-client-enabled"

  def clientImpl() = Mux.client
  def serverImpl() =
    Mux.server.withCompressionPreferences
      .compression(CompressionLevel.Desired, compressor)
      .withCompressionPreferences.decompression(CompressionLevel.Desired, decompressor)
}

class ZstdCompressionServerDisabledTest extends ZstdCompressingEndToEndTest {
  def implName: String = "compression-server-enabled"

  def clientImpl() =
    Mux.client.withCompressionPreferences
      .compression(CompressionLevel.Desired, compressor)
      .withCompressionPreferences.decompression(CompressionLevel.Desired, decompressor)
  def serverImpl() = Mux.server
}

abstract class MixedCompressingEndToEndTest extends AbstractEndToEndTest {
  override type ClientT = Mux.Client
  override type ServerT = Mux.Server

  override def skipWholeTest: Boolean = sys.props.contains("SKIP_FLAKY_TRAVIS")

  val compressor = Seq(Compression.lz4Compressor(highCompression = false))
  val decompressor = Seq(Compression.zstdDecompressor())
}

class MixedCompressionServerAcceptingTest extends MixedCompressingEndToEndTest {
  def implName: String = "compression-client-enabled"

  def clientImpl() =
    Mux.client.withCompressionPreferences.compression(CompressionLevel.Desired, compressor)
  def serverImpl() =
    Mux.server.withCompressionPreferences
      .compression(CompressionLevel.Accepted, compressor)
      .withCompressionPreferences.decompression(
        CompressionLevel.Desired,
        Seq(Compression.lz4Decompressor()))
}
