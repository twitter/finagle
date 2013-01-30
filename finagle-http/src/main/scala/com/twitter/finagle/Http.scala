package com.twitter.finagle

import com.twitter.finagle.builder.{Cluster, StaticCluster}
import com.twitter.finagle.client._
import com.twitter.finagle.dispatch.{SerialServerDispatcher, SerialClientDispatcher}
import com.twitter.finagle.netty3._
import com.twitter.finagle.server._
import com.twitter.finagle.stats.StatsReceiver
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
    val cluster = StaticCluster[SocketAddress](Seq(addr))
    val req = http.RequestBuilder().url(url).buildGet()
    val service = newClient(cluster).toService
    service(req) ensure {
      service.close()
    }
  }
}

object HttpBinder extends DefaultBinder[HttpRequest, HttpResponse, HttpRequest, HttpResponse](
  new Netty3Transporter(http.Http().client(ClientCodecConfig("httpclient")).pipelineFactory),
  new SerialClientDispatcher(_)
)

object HttpClient extends DefaultClient[HttpRequest, HttpResponse](
  HttpBinder, DefaultPool[HttpRequest, HttpResponse]()
) with HttpRichClient


object HttpListener extends Netty3Listener[HttpResponse, HttpRequest](
  http.Http().server(ServerCodecConfig("httpserver", new SocketAddress{})).pipelineFactory
)

object HttpServer extends DefaultServer[HttpRequest, HttpResponse, HttpResponse, HttpRequest](
  HttpListener, new SerialServerDispatcher(_, _)
)

object Http extends Client[HttpRequest, HttpResponse] with HttpRichClient 
    with Server[HttpRequest, HttpResponse]
{
  def newClient(cluster: Cluster[SocketAddress]): ServiceFactory[HttpRequest, HttpResponse] =
    HttpClient.newClient(cluster)

  def serve(addr: SocketAddress, service: ServiceFactory[HttpRequest, HttpResponse]): ListeningServer =
    HttpServer.serve(addr, service)
}
