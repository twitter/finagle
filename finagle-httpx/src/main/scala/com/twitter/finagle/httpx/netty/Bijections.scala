package com.twitter.finagle.httpx.netty

import com.twitter.finagle.httpx.{Status, Version, Method, Request, Response}
import com.twitter.finagle.netty3.{BufChannelBuffer, ChannelBufferBuf}
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

  implicit val versionFromNetty = new Injection[HttpVersion, Version] {
    def apply(v: HttpVersion) = v match {
      case HttpVersion.HTTP_1_1 => Version.Http11
      case HttpVersion.HTTP_1_0 => Version.Http10
    }
  }

  // Method

  implicit val methodToNetty = new Injection[Method, HttpMethod] {
    import HttpMethod._
    import Method._

    def apply(m: Method) = m match {
      case Get => GET
      case Post => POST
      case Put => PUT
      case Head => HEAD
      case Patch => PATCH
      case Delete => DELETE
      case Trace => TRACE
      case Connect => CONNECT
      case Options => OPTIONS
      case Method(name) => new HttpMethod(name)
    }
  }

  implicit val methodFromNetty = new Injection[HttpMethod, Method] {
    import HttpMethod._
    import Method._

    def apply(m: HttpMethod) = m match {
      case GET => Get
      case POST => Post
      case PUT => Put
      case HEAD => Head
      case PATCH => Patch
      case DELETE => Delete
      case TRACE => Trace
      case CONNECT => Connect
      case OPTIONS => Options
      case method => Method(method.getName)
    }
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
