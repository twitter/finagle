package com.twitter.finagle.stream

import com.twitter.finagle.netty3.{BufChannelBuffer, ChannelBufferBuf}
import com.twitter.io.Buf

case class StreamRequest(
    method: StreamRequest.Method,
    uri: String,
    version: Version = Version(1, 1),
    headers: Seq[Header] = Nil,
    body: Buf = Buf.Empty
)

object StreamRequest {
  sealed trait Method

  object Method {
    case object Options extends Method
    case object Get extends Method
    case object Head extends Method
    case object Post extends Method
    case object Put extends Method
    case object Delete extends Method
    case object Trace extends Method
    case object Connect extends Method
    case object Patch extends Method
    case class Custom(name: String) extends Method

    /**
     * Create a method from a String. The input is case-insensitive.
     */
    def apply(name: String): Method = name.toUpperCase match {
      case "OPTIONS" => Method.Options
      case "GET" => Method.Get
      case "HEAD" => Method.Head
      case "POST" => Method.Post
      case "PUT" => Method.Put
      case "DELETE" => Method.Delete
      case "TRACE" => Method.Trace
      case "CONNECT" => Method.Connect
      case "PATCH" => Method.Patch
      case name => Method.Custom(name)
    }
  }
}
