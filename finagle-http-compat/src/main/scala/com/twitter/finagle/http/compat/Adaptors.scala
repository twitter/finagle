package com.twitter.finagle.http.compat

import com.twitter.finagle.http
import com.twitter.finagle.{Service, Filter}
import com.twitter.util.Future
import org.jboss.netty.handler.codec.{http => netty}
import java.net.InetSocketAddress

/**
 * A abstract filter that adapts an arbitrary service to a Finagle HTTP
 * service.
 */
abstract class Adaptor[Req, Rep]
  extends Filter[http.Request, http.Response, Req, Rep] {
  private[compat] def in(req: http.Request): Future[Req]
  private[compat] def out(rep: Rep): Future[http.Response]
  def apply(req: http.Request, next: Service[Req, Rep]): Future[http.Response] =
    in(req) flatMap(next) flatMap(out)
}

/**
 * An Adaptor for Netty HTTP services.
 */
object NettyAdaptor extends Adaptor[netty.HttpRequest, netty.HttpResponse] {
  val NoStreaming = new IllegalArgumentException("this service doesn't support streaming")

  private[compat] def in(req: http.Request): Future[netty.HttpRequest] =
    if (req.isChunked) Future.exception(NoStreaming)
    else Future.value(req.httpRequest)

  private[compat] def out(r: netty.HttpResponse): Future[http.Response] =
    if (r.isChunked) Future.exception(NoStreaming)
    else Future.value(new http.Response { val httpResponse = r })
}

/**
 * A abstract filter to adapt HTTP clients into arbitrary clients.
 */
abstract class ClientAdaptor[Req, Rep]
  extends Filter[Req, Rep, http.Request, http.Response] {
  private[compat] def in(req: Req): Future[http.Request]
  private[compat] def out(rep: http.Response): Future[Rep]
  def apply(req: Req, next: Service[http.Request, http.Response]): Future[Rep] =
    in(req) flatMap(next) flatMap(out)
}

/**
 * A ClientAdaptor for Netty clients.
 */
object NettyClientAdaptor extends ClientAdaptor[netty.HttpRequest, netty.HttpResponse] {
  val NoStreaming = new IllegalArgumentException("this client doesn't support streaming")

  private[compat] def in(req: netty.HttpRequest): Future[http.Request] =
    if (req.isChunked) Future.exception(NoStreaming)
    else Future.value(new http.Request {
      val httpRequest = req
      lazy val remoteSocketAddress = new InetSocketAddress(0)
    })

  private[compat] def out(rep: http.Response): Future[netty.HttpResponse] =
    if (rep.isChunked) Future.exception(NoStreaming)
    else Future.value(rep.httpResponse)
}
