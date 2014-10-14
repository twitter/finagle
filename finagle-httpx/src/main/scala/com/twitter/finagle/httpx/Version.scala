package com.twitter.finagle.httpx

sealed trait Version

/** Scala aliases for HttpVersion.  Java users should use Netty's HttpVersion */
object Version {
  case object Http10 extends Version
  case object Http11 extends Version
}
