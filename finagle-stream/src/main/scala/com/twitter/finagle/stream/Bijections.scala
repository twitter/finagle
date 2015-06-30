package com.twitter.finagle.stream

import com.twitter.finagle.netty3.{BufChannelBuffer, ChannelBufferBuf}
import org.jboss.netty.handler.codec.http._
import scala.collection.JavaConverters._

private[stream] trait From[A, B] {
  def apply(a: A): B
}

private[stream] object Bijections {
  /**
   * Convert an A to a B.
   */
  def from[A, B](a: A)(implicit ev: From[A, B]): B =
    ev.apply(a)

  // Version

  implicit val toNettyVersion = new From[Version, HttpVersion] {
    def apply(version: Version) =
      HttpVersion.valueOf(s"HTTP/${version.major}.${version.minor}")
  }

  implicit val fromNettyVersion = new From[HttpVersion, Version] {
    def apply(version: HttpVersion) =
      Version(version.getMajorVersion, version.getMinorVersion)
  }

  // Method

  implicit val toNettyMethod = new From[StreamRequest.Method, HttpMethod] {
    def apply(method: StreamRequest.Method) =
      method match {
        case StreamRequest.Method.Custom(name) => HttpMethod.valueOf(name)
        case _ => HttpMethod.valueOf(method.toString.toUpperCase)
      }
  }

  implicit val fromNettyMethod = new From[HttpMethod, StreamRequest.Method] {
    def apply(method: HttpMethod) =
      StreamRequest.Method(method.getName)
  }

  // Headers

  implicit val fromNettyHeaders: From[HttpHeaders, Seq[Header]] =
    new From[HttpHeaders, Seq[Header]] {
      def apply(headers: HttpHeaders) =
        headers.iterator.asScala.map(e => Header(e.getKey, e.getValue)).toSeq
    }

  // Request

  implicit val fromNettyRequest: From[HttpRequest, StreamRequest] =
    new From[HttpRequest, StreamRequest] {
      def apply(req: HttpRequest) =
        StreamRequest(
          from(req.getMethod),
          req.getUri,
          from(req.getProtocolVersion),
          from(req.headers),
          ChannelBufferBuf.Owned(req.getContent)
        )
    }

  implicit val toNettyRequest: From[StreamRequest, HttpRequest] =
    new From[StreamRequest, HttpRequest] {
      def apply(req: StreamRequest) = {
        val httpReq = new DefaultHttpRequest(
          from(req.version),
          from(req.method),
          req.uri
        )
        req.headers.foreach(h => httpReq.headers.add(h.key, h.value))
        httpReq.setContent(BufChannelBuffer(req.body))
        httpReq
      }
    }
}
