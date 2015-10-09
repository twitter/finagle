package com.twitter.finagle.httpx.compat

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
  extends Filter[httpx.Request, httpx.Response, Req, Rep] {
  private[compat] def in(req: httpx.Request): Future[Req]
  private[compat] def out(rep: Rep): Future[httpx.Response]
  def apply(req: httpx.Request, next: Service[Req, Rep]): Future[httpx.Response] =
    in(req) flatMap(next) flatMap(out)
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

/**
 * A abstract filter to adapt HTTPx clients into arbitrary clients.
 */
abstract class ClientAdaptor[Req, Rep]
  extends Filter[Req, Rep, httpx.Request, httpx.Response] {
  private[compat] def in(req: Req): Future[httpx.Request]
  private[compat] def out(rep: httpx.Response): Future[Rep]
  def apply(req: Req, next: Service[httpx.Request, httpx.Response]): Future[Rep] =
    in(req) flatMap(next) flatMap(out)
}

/**
 * A ClientAdaptor for Netty clients.
 */
object NettyClientAdaptor extends ClientAdaptor[netty.HttpRequest, netty.HttpResponse] {
  val NoStreaming = new IllegalArgumentException("this client doesn't support streaming")

  private[compat] def in(req: netty.HttpRequest): Future[httpx.Request] =
    if (req.isChunked) Future.exception(NoStreaming)
    else Future.value(new httpx.Request {
      val httpRequest = req
      lazy val remoteSocketAddress = new InetSocketAddress(0)
    })

  private[compat] def out(rep: httpx.Response): Future[netty.HttpResponse] =
    if (rep.isChunked) Future.exception(NoStreaming)
    else Future.value(rep.httpResponse)
}
