package com.twitter.finagle.postgresql.types

import java.nio.charset.Charset
import java.nio.charset.StandardCharsets

import com.twitter.io.Buf

/**
 * A simple wrapper for json data (either JSON or JSONB).
 *
 * This is meant to allow json libraries to parse out json without incurring the cost of
 * going through its `String` representation.
 *
 * Most libraries should be able to extract their AST from this, but care must be taken
 * in regards to the character set.
 */
case class Json(value: Buf, charset: Charset) {

  /**
   * Whether the underlying bytes are encoded in UTF8.
   *
   * This is necessary since several JSON libraries assume the underlying json is encoded in utf8.
   * It's usually the case, but when it's not, then it's important to fallback
   * to using `jsonString` for parsing.
   */
  val isUtf8: Boolean = charset == StandardCharsets.UTF_8

  lazy val jsonString: String = Buf.decodeString(value, charset)
  lazy val jsonByteArray: Array[Byte] = Buf.ByteArray.Owned.extract(value)
  def jsonByteBuffer: java.nio.ByteBuffer = Buf.ByteBuffer.Owned.extract(value)
}
