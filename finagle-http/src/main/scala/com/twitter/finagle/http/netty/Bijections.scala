package com.twitter.finagle.http.netty

import com.twitter.finagle.http.{Status, Version, Method, Request, Response}
import java.net.InetSocketAddress
import org.jboss.netty.handler.codec.http.{
  HttpVersion, HttpResponseStatus, HttpMethod, HttpRequest, HttpResponse
}

// TODO Use bijection-core when bijection.Conversion is contravariant in A.
// See: github.com/twitter/bijection/pull/180.

private[finagle] trait Injection[A, B] {
  def apply(a: A): B
}

object Bijections {
  def from[A, B](a: A)(implicit I: Injection[A, B]): B = I.apply(a)

  // Version

  implicit val versionToNetty = new Injection[Version, HttpVersion] {
    def apply(v: Version) = v match {
      case Version.Http11 => HttpVersion.HTTP_1_1
      case Version.Http10 => HttpVersion.HTTP_1_0
    }
  }

  // Note: netty 3's HttpVersion allows arbitrary protocol names so the bijection
  // here is a lie since finagle-http's Version can only represent HTTP/1.0 and HTTP/1.1.
  // However, netty 3 only decodes HTTP/1.0 and HTTP/1.1 messages so whatever came over
  // the wire at least looks like HTTP/1.x, so we take a guess in the base case.
  implicit val versionFromNetty = new Injection[HttpVersion, Version] {
    def apply(v: HttpVersion) = v match {
      case HttpVersion.HTTP_1_1 => Version.Http11
      case HttpVersion.HTTP_1_0 => Version.Http10
      case _ => Version.Http11
    }
  }

  // Method

  implicit val methodToNetty = new Injection[Method, HttpMethod] {
    def apply(m: Method): HttpMethod = HttpMethod.valueOf(m.toString)
  }

  implicit val methodFromNetty = new Injection[HttpMethod, Method] {
    def apply(m: HttpMethod): Method = Method(m.getName)
  }

  // Status

  implicit val statusToNetty = new Injection[Status, HttpResponseStatus] {
    def apply(s: Status) = HttpResponseStatus.valueOf(s.code)
  }

  implicit val statusFromNetty = new Injection[HttpResponseStatus, Status] {
    def apply(s: HttpResponseStatus) = Status.fromCode(s.getCode)
  }

  // Request

  implicit val requestToNetty = new Injection[Request, HttpRequest] {
    def apply(r: Request): HttpRequest = r.httpRequest
  }

  implicit val requestFromNetty = new Injection[HttpRequest, Request] {
    def apply(r: HttpRequest): Request = new Request {
      val httpRequest = r
      lazy val remoteSocketAddress = new InetSocketAddress(0)
    }
  }

  // Response

  implicit val responseFromNetty = new Injection[HttpResponse, Response] {
    def apply(r: HttpResponse): Response = Response(r)
  }

  implicit val responseToNetty = new Injection[Response, HttpResponse] {
    def apply(r: Response): HttpResponse = r.httpResponse
  }
}
