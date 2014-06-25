package com.twitter.finagle

import com.twitter.conversions.storage._
import com.twitter.finagle.client._
import com.twitter.finagle.dispatch.SerialServerDispatcher
import com.twitter.finagle.http.codec.{HttpClientDispatcher, HttpServerDispatcher}
import com.twitter.finagle.http.HttpTransport
import com.twitter.finagle.http.filter.DtabFilter
import com.twitter.finagle.http.{HttpServerTracingFilter, HttpClientTracingFilter}
import com.twitter.finagle.netty3._
import com.twitter.finagle.server._
import com.twitter.finagle.ssl.Ssl
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Future, StorageUnit}
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

object HttpTransporter extends Netty3Transporter[Any, Any](
  "http",
  http.Http().client(ClientCodecConfig("httpclient")).pipelineFactory
)

object HttpClient extends DefaultClient[HttpRequest, HttpResponse](
  name = "http",
  endpointer = Bridge[Any, Any, HttpRequest, HttpResponse](
    HttpTransporter, new HttpClientDispatcher(_))
) with HttpRichClient

object HttpListener extends Netty3Listener[Any, Any](
  "http",
  http.Http().server(ServerCodecConfig("httpserver", new SocketAddress{})).pipelineFactory
)

object HttpServer
extends DefaultServer[HttpRequest, HttpResponse, Any, Any](
  "http", HttpListener,
  {
    val dtab = new DtabFilter[HttpRequest, HttpResponse]
    val tracingFilter = new HttpServerTracingFilter[HttpRequest, HttpResponse]("http")
    (t, s) => new HttpServerDispatcher(
      new HttpTransport(t),
      tracingFilter andThen dtab andThen s
    )
  }
)

object Http extends Client[HttpRequest, HttpResponse] with HttpRichClient
    with Server[HttpRequest, HttpResponse]
{
  def newClient(name: Name, label: String): ServiceFactory[HttpRequest, HttpResponse] = {
    val tracingFilter = new HttpClientTracingFilter[HttpRequest, HttpResponse](label)
    tracingFilter andThen HttpClient.newClient(name, label)
  }

  def serve(addr: SocketAddress, service: ServiceFactory[HttpRequest, HttpResponse]): ListeningServer = {
    HttpServer.serve(addr, service)
  }
}

package exp {

  private[exp] object Http {
    object StackParams {
      case class MaxRequestSize(size: StorageUnit)
      implicit object MaxRequestSize extends Stack.Param[MaxRequestSize] {
        val default = MaxRequestSize(5.megabytes)
      }

      case class MaxResponseSize(size: StorageUnit)
      implicit object MaxResponseSize extends Stack.Param[MaxResponseSize] {
        val default = MaxResponseSize(5.megabytes)
      }

      def applyToCodec(params: Stack.Params, codec: http.Http): http.Http =
        codec
          .maxRequestSize(params[MaxRequestSize].size)
          .maxResponseSize(params[MaxResponseSize].size)
    }
  }

  private[finagle]
  class HttpClient(client: StackClient[HttpRequest, HttpResponse, Any, Any])
    extends StackClientLike[HttpRequest, HttpResponse, Any, Any, HttpClient](client) {

    protected def newInstance(client: StackClient[HttpRequest, HttpResponse, Any, Any]) =
      new HttpClient(client)

    def withTls(cfg: Netty3TransporterTLSConfig): HttpClient =
      configured((Transport.TLSEngine(Some(cfg.newEngine))))
        .configured(Transporter.TLSHostname(cfg.verifyHost))
        .transformed { stk => http.TlsFilter.module +: stk }

    def withTls(hostname: String): HttpClient =
      withTls(new Netty3TransporterTLSConfig({ () => Ssl.client() }, Some(hostname)))

    def withTlsWithoutValidation(): HttpClient =
      configured(Transport.TLSEngine(Some({ () => Ssl.clientWithoutCertificateValidation() })))

    def withMaxRequestSize(size: StorageUnit): HttpClient =
      configured(Http.StackParams.MaxRequestSize(size))

    def withMaxResponseSize(size: StorageUnit): HttpClient =
      configured(Http.StackParams.MaxResponseSize(size))
  }

  object HttpClient extends HttpClient(new StackClient[HttpRequest, HttpResponse, Any, Any] {
    protected val newTransporter: Stack.Params => Transporter[Any, Any] = { prms =>
      val param.Label(label) = prms[param.Label]
      val httpPipeline =
        Http.StackParams.applyToCodec(prms, http.Http())
          .client(ClientCodecConfig(label))
          .pipelineFactory
      Netty3Transporter(httpPipeline, prms)
    }

    protected val newDispatcher: Stack.Params => Dispatcher =
      Function.const(new HttpClientDispatcher(_))
  })

  private[finagle]
  class HttpServer(
    server: StackServer[HttpRequest, HttpResponse, Any, Any]
  ) extends StackServerLike[HttpRequest, HttpResponse, Any, Any, HttpServer](server) {
    protected def newInstance(server: StackServer[HttpRequest, HttpResponse, Any, Any]) =
      new HttpServer(server)

    def withTls(cfg: Netty3ListenerTLSConfig): HttpServer =
      configured(Transport.TLSEngine(Some(cfg.newEngine)))

    def withMaxRequestSize(size: StorageUnit): HttpServer =
      configured(Http.StackParams.MaxRequestSize(size))

    def withMaxResponseSize(size: StorageUnit): HttpServer =
      configured(Http.StackParams.MaxResponseSize(size))
  }

  object HttpServer extends HttpServer(new StackServer[HttpRequest, HttpResponse, Any, Any] {
    protected val newListener: Stack.Params => Listener[Any, Any] = { prms =>
      val param.Label(label) = prms[param.Label]
      val httpPipeline =
        Http.StackParams.applyToCodec(prms, http.Http())
          .server(ServerCodecConfig(label, new SocketAddress{}))
          .pipelineFactory
      Netty3Listener(httpPipeline, prms)
    }

    protected val newDispatcher: Stack.Params => Dispatcher = {
      val dtab = new DtabFilter[HttpRequest, HttpResponse]
      val tracingFilter = new HttpServerTracingFilter[HttpRequest, HttpResponse]("http")
      Function.const((t, s) => new HttpServerDispatcher(
        new HttpTransport(t),
        tracingFilter andThen dtab andThen s
      ))
    }
  })
}
