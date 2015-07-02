package com.twitter.finagle.httpx

/**
 * Represents the HTTP version.
 *
 * For Java-friendly enums, see [[com.twitter.finagle.httpx.Versions]].
 */
sealed abstract class Version

object Version {
  case object Http10 extends Version
  case object Http11 extends Version
}
