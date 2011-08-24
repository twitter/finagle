package com.twitter.finagle.http

import org.jboss.netty.handler.codec.http.HttpVersion

/** Scala aliases for HttpVersion.  Java users should use Netty's HttpVersion */
object Version {
  val Http10 = HttpVersion.HTTP_1_0
  val Http11 = HttpVersion.HTTP_1_1
}
