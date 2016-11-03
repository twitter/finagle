package com.twitter.finagle.http

/**
 * Represents the HTTP version.
 *
 * For Java-friendly enums, see [[com.twitter.finagle.http.Versions]].
 */
sealed abstract class Version

object Version {
  /** HTTP 1.0 */
  case object Http10 extends Version {
    override def toString: String = "HTTP/1.0"
  }

  /** HTTP 1.1 */
  case object Http11 extends Version {
    override def toString: String = "HTTP/1.1"
  }
}
