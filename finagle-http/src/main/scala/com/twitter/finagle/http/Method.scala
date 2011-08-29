package com.twitter.finagle.http

import org.jboss.netty.handler.codec.http.HttpMethod

/** Scala aliases for HttpMethod.  Java users should use Netty's HttpMethod. */
object Method {
  val Get     = HttpMethod.GET
  val Post    = HttpMethod.POST
  val Put     = HttpMethod.PUT
  val Head    = HttpMethod.HEAD
  val Patch   = HttpMethod.PATCH
  val Delete  = HttpMethod.DELETE
  val Trace   = HttpMethod.TRACE
  val Connect = HttpMethod.CONNECT
}
