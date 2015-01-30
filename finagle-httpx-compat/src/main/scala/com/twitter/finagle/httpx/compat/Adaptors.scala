package com.twitter.finagle.httpx.compat

import com.twitter.finagle.http
import com.twitter.finagle.httpx
import com.twitter.finagle.{Service, Filter}
import com.twitter.util.Future
import org.jboss.netty.handler.codec.{http => netty}
import java.net.InetSocketAddress

/**
 * A abstract filter that adapts an arbitrary service to a Finagle HTTPx
 * service.
 */
abstract class Adaptor[Req, Rep]
  extends Filter[httpx.Ask, httpx.Response, Req, Rep] {
  private[compat] def in(req: httpx.Ask): Future[Req]
  private[compat] def out(rep: Rep): Future[httpx.Response]
  def apply(req: httpx.Ask, next: Service[Req, Rep]): Future[httpx.Response] =
    in(req) flatMap(next) flatMap(out)
}

/**
 * An Adaptor for the older generation of HTTP services.
 */
object HttpAdaptor extends Adaptor[http.Ask, http.Response] {
  private[compat] def in(r: httpx.Ask): Future[http.Ask] = {
    val req = new http.Ask {
      val httpAsk = r.httpAsk
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

  private[compat] def in(req: httpx.Ask): Future[netty.HttpRequest] =
    if (req.isChunked) Future.exception(NoStreaming)
    else Future.value(req.httpAsk)

  private[compat] def out(r: netty.HttpResponse): Future[httpx.Response] =
    if (r.isChunked) Future.exception(NoStreaming)
    else Future.value(new httpx.Response { val httpResponse = r })
}

/**
 * A abstract filter to adapt HTTPx clients into arbitrary clients.
 */
abstract class ClientAdaptor[Req, Rep]
  extends Filter[Req, Rep, httpx.Ask, httpx.Response] {
  private[compat] def in(req: Req): Future[httpx.Ask]
  private[compat] def out(rep: httpx.Response): Future[Rep]
  def apply(req: Req, next: Service[httpx.Ask, httpx.Response]): Future[Rep] =
    in(req) flatMap(next) flatMap(out)
}

/**
 * A ClientAdaptor for Netty clients.
 */
object NettyClientAdaptor extends ClientAdaptor[netty.HttpRequest, netty.HttpResponse] {
  val NoStreaming = new IllegalArgumentException("this client doesn't support streaming")

  private[compat] def in(req: netty.HttpRequest): Future[httpx.Ask] =
    if (req.isChunked) Future.exception(NoStreaming)
    else Future.value(new httpx.Ask {
      val httpAsk = req
      lazy val remoteSocketAddress = new InetSocketAddress(0)
    })

  private[compat] def out(rep: httpx.Response): Future[netty.HttpResponse] =
    if (rep.isChunked) Future.exception(NoStreaming)
    else Future.value(rep.httpResponse)
}
