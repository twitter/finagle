package com.twitter.finagle.memcached.protocol.text

import com.twitter.io.Buf

private[finagle] trait MessageEncoder[T] {

  /**
   * Encode a message of type `T` to a `Buf`.
   */
  def encode(message: T): Buf
}
