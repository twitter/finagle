package com.twitter.finagle.http

/**
 * Represents the HTTP version.
 */
final case class Version private (major: Int, minor: Int) {

  /** String representation of the HTTP version */
  val versionString: String = s"HTTP/${major}.${minor}"

  override def toString: String = versionString
}

object Version {

  /** HTTP 1.0 */
  val Http10: Version = Version(1, 0)

  /** HTTP 1.1 */
  val Http11: Version = Version(1, 1)
}
