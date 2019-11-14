package com.twitter.finagle.mux

import com.twitter.conversions.StorageUnitOps._
import com.twitter.finagle.Mux.param.{MaxFrameSize, CompressionPreferences}
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

  private[this] val compressor = Seq(Compression.lz4Compressor(highCompression = true))
  private[this] val decompressor = Seq(Compression.lz4Decompressor())
  private[this] def preferences(level: CompressionLevel) = CompressionPreferences(
    Compression.LocalPreferences(
      Compression.LocalSetting(
        level,
        compressor
      ),
      Compression.LocalSetting(
        level,
        decompressor
      )
    )
  )

  protected val desired = preferences(CompressionLevel.Desired)

  protected val accepted = preferences(CompressionLevel.Accepted)
}

class CompressionEnabledTest extends CompressingEndToEndTest {
  def implName: String = "compression-enabled"

  def clientImpl() = Mux.client.configured(desired)
  def serverImpl() = Mux.server.configured(desired)
}

class CompressionClientAcceptingTest extends CompressingEndToEndTest {
  def implName: String = "compression-client-enabled"

  def clientImpl() = Mux.client.configured(accepted)
  def serverImpl() = Mux.server.configured(desired)
}

class CompressionServerAcceptingTest extends CompressingEndToEndTest {
  def implName: String = "compression-client-enabled"

  def clientImpl() = Mux.client.configured(desired)
  def serverImpl() = Mux.server.configured(accepted)
}

class CompressionClientDisabledTest extends CompressingEndToEndTest {
  def implName: String = "compression-client-enabled"

  def clientImpl() = Mux.client
  def serverImpl() = Mux.server.configured(desired)
}

class CompressionServerDisabledTest extends CompressingEndToEndTest {
  def implName: String = "compression-server-enabled"

  def clientImpl() = Mux.client.configured(desired)
  def serverImpl() = Mux.server
}
