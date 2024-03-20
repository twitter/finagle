package com.twitter.finagle.memcached.compressing.param

import com.twitter.finagle.Stack
import com.twitter.finagle.memcached.compressing.scheme.CompressionScheme
import com.twitter.finagle.memcached.compressing.scheme.Uncompressed

case class CompressionParam(scheme: CompressionScheme) {
  def mk(): (CompressionParam, Stack.Param[CompressionParam]) =
    (this, CompressionParam.None)
}

object CompressionParam {
  implicit val None: Stack.Param[CompressionParam] =
    Stack.Param(CompressionParam(Uncompressed))
}
