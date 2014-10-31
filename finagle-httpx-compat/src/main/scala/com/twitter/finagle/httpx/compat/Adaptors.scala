package com.twitter.finagle.httpx.compat

import com.twitter.finagle.http
import com.twitter.finagle.httpx
import com.twitter.finagle.{Service, Filter}
import com.twitter.util.Future
import org.jboss.netty.handler.codec.{http => netty}

/**
 * A abstract filter that adapts an arbitrary service to a Finagle HTTPx
 * service.
 */
abstract class Adaptor[Req, Rep]
  extends Filter[httpx.Request, httpx.Response, Req, Rep] {
  private[compat] def in(req: httpx.Request): Future[Req]
  private[compat] def out(rep: Rep): Future[httpx.Response]
  def apply(req: httpx.Request, next: Service[Req, Rep]): Future[httpx.Response] =
    in(req) flatMap(next) flatMap(out)
}

/**
 * An Adaptor for the older generation of HTTP services.
 */
object HttpAdaptor extends Adaptor[http.Request, http.Response] {
  private[compat] def in(r: httpx.Request): Future[http.Request] = {
    val req = new http.Request {
      val httpRequest = r.httpRequest
      lazy val remoteSocketAddress = r.remoteSocketAddress
      override val reader = r.reader
      override val writer = r.writer
    }
    if (!r.isChunked) req.setContent(r.getContent)
    Future.value(req)
  }

  private[compat] def out(r: http.Response): Future[httpx.Response] = {
    val res = new httpx.Response {
      val httpResponse = r.httpResponse
      override val reader = r.reader
      override val writer = r.writer
    }
    if (!r.isChunked) res.setContent(r.getContent)
    Future.value(res)
  }
}

/**
 * An Adaptor for Netty HTTP services.
 */
object NettyAdaptor extends Adaptor[netty.HttpRequest, netty.HttpResponse] {
  val NoStreaming = new IllegalArgumentException("this service doesn't support streaming")

  private[compat] def in(req: httpx.Request): Future[netty.HttpRequest] =
    if (req.isChunked) Future.exception(NoStreaming)
    else Future.value(req.httpRequest)

  private[compat] def out(r: netty.HttpResponse): Future[httpx.Response] =
    if (r.isChunked) Future.exception(NoStreaming)
    else Future.value(new httpx.Response { val httpResponse = r })
}
