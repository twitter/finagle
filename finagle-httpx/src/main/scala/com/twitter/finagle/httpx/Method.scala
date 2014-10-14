package com.twitter.finagle.httpx

import org.jboss.netty.handler.codec.http.HttpMethod

sealed trait Method

/** Scala aliases for HttpMethod.  Java users should use Netty's HttpMethod. */
object Method {
  case object Get extends Method
  case object Post extends Method
  case object Put extends Method
  case object Head extends Method
  case object Patch extends Method
  case object Delete extends Method
  case object Trace extends Method
  case object Connect extends Method
  case object Options extends Method

  private case class Custom(name: String) extends Method

  def apply(name: String): Method = name.toUpperCase match {
    case "GET" => Get
    case "POST" => Post
    case "PUT" => Put
    case "HEAD" => Head
    case "PATCH" => Patch
    case "DELETE" => Delete
    case "TRACE" => Trace
    case "CONNECT" => Connect
    case "OPTIONS" => Options
    case method => Custom(method)
  }

  def unapply(method: Method): Option[String] = method match {
    case Custom(name) => Some(name)
    case _ => None
  }
}
