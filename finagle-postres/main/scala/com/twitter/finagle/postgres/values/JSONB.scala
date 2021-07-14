package com.twitter.finagle.postgres.values

import java.nio.charset.Charset

object JSONB {
  def apply(string: String, charset: Charset = Charset.defaultCharset): JSONB = new JSONB(string.getBytes(charset))

  def stringify(jsonb: JSONB, charset: Charset = Charset.defaultCharset): String = new String(jsonb.bytes, charset)
}

case class JSONB(bytes: Array[Byte]) extends AnyVal

