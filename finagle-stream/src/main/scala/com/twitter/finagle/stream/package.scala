package com.twitter.finagle

import org.jboss.netty.handler.codec.http._
import com.twitter.finagle.netty3.{BufChannelBuffer, ChannelBufferBuf}

/**
 * Finagle-stream implements a rather peculiar protocol: it streams
 * discrete messages delineated by HTTP chunks. It isn't how we'd design
 * a protocol to stream messages, but we are stuck with it for legacy
 * reasons.
 * 
 * Finagle-stream sessions are also ``one-shot``: each session handles
 * exactly one stream. The session terminates together with the stream.
 */
package object stream {
  import Bijections._

  /**
   * Indicates that a stream has ended.
   */
  object EOF extends Exception

  /**
   * HTTP header encoded as a string pair.
   */
  final class Header private(val key: String, val value: String) {
    override def toString: String = s"Header($key, $value)"
    override def equals(o: Any): Boolean = o match {
      case h: Header => h.key == key && h.value == value
      case _ => false
    }
  }

  object Header {
    implicit class Ops(val headers: Seq[Header]) extends AnyVal {
      /**
       * The value of the first header found matching this key, or None.
       */
      def first(key: String): Option[String] =
        headers.find(_.key == key.toLowerCase).map(_.value)
    }

    def apply(key: String, value: Any): Header =
      new Header(key.toLowerCase, value.toString)
  }

  /**
   * Represents the HTTP version.
   */
  case class Version(major: Int, minor: Int)

  trait RequestType[Req] {
    def canonize(req: Req): StreamRequest
    def specialize(req: StreamRequest): Req
  }

  implicit val streamRequestType = new RequestType[StreamRequest] {
    def canonize(req: StreamRequest) = req
    def specialize(req: StreamRequest) = req
  }
}
