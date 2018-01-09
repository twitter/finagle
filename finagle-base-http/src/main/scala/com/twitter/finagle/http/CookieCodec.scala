package com.twitter.finagle.http

/**
 * Interface for encoding/decoding cookies.
 * This allows us to easily swap out N3 and N4 versions.
 */
private[finagle] abstract class CookieCodec {

  /**
   * Encode client cookies to a String
   */
  def encodeClient(cookies: Iterable[Cookie]): String

  /**
   * Encode a single server cookie to a String
   */
  def encodeServer(cookie: Cookie): String

  /**
   * Decode a client cookie header. Returns `Some[Iterable[Cookie]]`
   * if the decoding succeeded, and `None` otherwise.
   */
  def decodeClient(header: String): Option[Iterable[Cookie]]

  /**
   * Decode a server cookie header. Returns `Some[Iterable[Cookie]]`
   * if the decoding succeeded, and `None` otherwise.
   */
  def decodeServer(header: String): Option[Iterable[Cookie]]
}
