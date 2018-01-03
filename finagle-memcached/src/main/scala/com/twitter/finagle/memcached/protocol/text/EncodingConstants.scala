package com.twitter.finagle.memcached.protocol.text

import java.nio.charset.StandardCharsets

private object EncodingConstants {
  val SPACE = " ".getBytes(StandardCharsets.UTF_8)
  val DELIMITER = "\r\n".getBytes(StandardCharsets.UTF_8)
  val END = "END".getBytes(StandardCharsets.UTF_8)
}
