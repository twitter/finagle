package com.twitter.finagle.memcached.compressing

import com.twitter.finagle.Stack
import com.twitter.finagle.memcached.compressing.param.CompressionParam
import com.twitter.finagle.memcached.compressing.scheme.CompressionScheme

/**
 * A collection of methods for configuring compression schemes of Memcached clients
 */
trait WithCompressionScheme[A <: Stack.Parameterized[A]] { self: Stack.Parameterized[A] =>

  /**
   * Defines the compression function to use for compression clients
   *
   * @note: Compression client does not support the following memcached operations
   *  1) append
   *  2) prepend
   *  3) replace
   */
  def withCompressionScheme(scheme: CompressionScheme): A =
    configured(CompressionParam(scheme))

}
