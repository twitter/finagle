package com.twitter.finagle

import com.twitter.finagle.client._
import com.twitter.finagle.dispatch.SerialServerDispatcher
import com.twitter.finagle.http.codec.HttpClientDispatcher
import com.twitter.finagle.netty3._
import com.twitter.finagle.server._
import com.twitter.util.Future
import java.net.{InetSocketAddress, SocketAddress}
import org.jboss.netty.handler.codec.http._

trait HttpRichClient { self: Client[HttpRequest, HttpResponse] =>
  def fetchUrl(url: String): Future[HttpResponse] = fetchUrl(new java.net.URL(url))
  def fetchUrl(url: java.net.URL): Future[HttpResponse] = {
    val addr = {
      val port = if (url.getPort < 0) url.getDefaultPort else url.getPort
      new InetSocketAddress(url.getHost, port)
    }
    val group = Group[SocketAddress](addr)
    val req = http.RequestBuilder().url(url).buildGet()
    val service = newClient(group).toService
    service(req) ensure {
      service.close()
    }
  }
}

object HttpTransporter extends Netty3Transporter[HttpRequest, HttpResponse](
  "http",
  http.Http()
    .enableTracing(true)
    .client(ClientCodecConfig("httpclient")).pipelineFactory
)

object HttpClient extends DefaultClient[HttpRequest, HttpResponse](
  name = "http",
  endpointer = Bridge[HttpRequest, HttpResponse, HttpRequest, HttpResponse](
    HttpTransporter, new HttpClientDispatcher(_))
) with HttpRichClient

object HttpListener extends Netty3Listener[HttpResponse, HttpRequest](
  "http",
  http.Http()
    .enableTracing(true)
    .server(ServerCodecConfig("httpserver", new SocketAddress{})).pipelineFactory
)

object HttpServer extends DefaultServer[HttpRequest, HttpResponse, HttpResponse, HttpRequest](
  "http", HttpListener, new SerialServerDispatcher(_, _)
)

object Http extends Client[HttpRequest, HttpResponse] with HttpRichClient
    with Server[HttpRequest, HttpResponse]
{
  def newClient(group: Group[SocketAddress]): ServiceFactory[HttpRequest, HttpResponse] =
    HttpClient.newClient(group)

  def serve(addr: SocketAddress, service: ServiceFactory[HttpRequest, HttpResponse]): ListeningServer =
    HttpServer.serve(addr, service)
}
