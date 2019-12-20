package com.twitter.finagle.mux

import com.twitter.finagle.{Mux, Stack}
import com.twitter.finagle.mux.transport.CompressionLevel
import com.twitter.finagle.mux.transport.Compression.{ByteTransformer, LocalSetting}

/**
 * A collection of methods for configuring compression for ThriftMux clients and servers.
 *
 * @tparam A a ThriftMux.Client or ThriftMux.Server to configure.
 */
class CompressionParams[A <: Stack.Parameterized[A]](self: Stack.Parameterized[A]) {

  /**
   * Configures the compression mode of this client.
   *
   * @param level       indicates whether the client is able to compress, and if it is able, whether
   *                    it prefers to compress or not.
   * @param compressors the transformers that specify which compression formats it can use to
   *                    compress.
   */
  def compression(level: CompressionLevel, compressors: Seq[ByteTransformer]): A = {
    val prefs = self.params[Mux.param.CompressionPreferences].compressionPreferences
    val updated = prefs.copy(compression = LocalSetting(level, compressors))
    self.configured(Mux.param.CompressionPreferences(updated))
  }

  /**
   * Configures the decompression mode of this client.
   *
   * @param level         indicates whether the client is able to compress, and if it is able,
   *                      whether it prefers to compress or not.
   * @param decompressors the transformers that specify which compression formats it can use to
   *                      compress.
   */
  def decompression(level: CompressionLevel, decompressors: Seq[ByteTransformer]): A = {
    val prefs = self.params[Mux.param.CompressionPreferences].compressionPreferences
    val updated = prefs.copy(decompression = LocalSetting(level, decompressors))
    self.configured(Mux.param.CompressionPreferences(updated))
  }
}
