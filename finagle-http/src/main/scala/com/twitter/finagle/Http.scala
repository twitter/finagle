package com.twitter.finagle

import com.twitter.conversions.storage._
import com.twitter.finagle.client._
import com.twitter.finagle.dispatch.SerialServerDispatcher
import com.twitter.finagle.http.codec.{HttpClientDispatcher, HttpServerDispatcher}
import com.twitter.finagle.http.filter.DtabFilter
import com.twitter.finagle.http._
import com.twitter.finagle.netty3._
import com.twitter.finagle.server._
import com.twitter.finagle.ssl.Ssl
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.tracing._
import com.twitter.util.{Future, StorageUnit}
import java.net.{InetSocketAddress, SocketAddress}
import org.jboss.netty.handler.codec.http._

/**
 * A rich client with a *very* basic URL fetcher. (It does not handle
 * redirects, does not have a cookie jar, etc.)
 */
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

/**
 * Http protocol support, including client and server.
 */
object Http extends Client[HttpRequest, HttpResponse] with HttpRichClient
    with Server[HttpRequest, HttpResponse] {

  object param {
    case class MaxRequestSize(size: StorageUnit)
    implicit object MaxRequestSize extends Stack.Param[MaxRequestSize] {
      val default = MaxRequestSize(5.megabytes)
    }

    case class MaxResponseSize(size: StorageUnit)
    implicit object MaxResponseSize extends Stack.Param[MaxResponseSize] {
      val default = MaxResponseSize(5.megabytes)
    }

    private[Http] def applyToCodec(params: Stack.Params, codec: http.Http): http.Http =
      codec
        .maxRequestSize(params[MaxRequestSize].size)
        .maxResponseSize(params[MaxResponseSize].size)
  }

  object Client {
    val stack: Stack[ServiceFactory[HttpRequest, HttpResponse]] = StackClient.newStack
  }

  case class Client(
    stack: Stack[ServiceFactory[HttpRequest, HttpResponse]] = Client.stack
      .replace(TraceInitializerFilter.role, new HttpClientTraceInitializer[HttpRequest, HttpResponse]),
    params: Stack.Params = StackClient.defaultParams
  ) extends StdStackClient[HttpRequest, HttpResponse, Client] {
    protected type In = Any
    protected type Out = Any

    protected def newTransporter(): Transporter[Any, Any] = {
      val com.twitter.finagle.param.Label(label) = params[com.twitter.finagle.param.Label]
      val httpPipeline =
        param.applyToCodec(params, http.Http())
          .client(ClientCodecConfig(label))
          .pipelineFactory
      Netty3Transporter(httpPipeline, params)
    }

    protected def copy1(
      stack: Stack[ServiceFactory[HttpRequest, HttpResponse]] = this.stack,
      params: Stack.Params = this.params
    ): Client = copy(stack, params)

    protected def newDispatcher(transport: Transport[Any, Any]): Service[HttpRequest, HttpResponse] =
      new HttpClientDispatcher(transport)

    def withTls(cfg: Netty3TransporterTLSConfig): Client =
      configured((Transport.TLSClientEngine(Some(cfg.newEngine))))
        .configured(Transporter.TLSHostname(cfg.verifyHost))
        .transformed { stk => http.TlsFilter.module +: stk }

    def withTls(hostname: String): Client =
      withTls(new Netty3TransporterTLSConfig({
        case inet: InetSocketAddress => Ssl.client(hostname, inet.getPort)
        case _ => Ssl.client()
      }, Some(hostname)))

    def withTlsWithoutValidation(): Client =
      configured(Transport.TLSClientEngine(Some({
        case inet: InetSocketAddress => Ssl.clientWithoutCertificateValidation(inet.getHostString, inet.getPort)
        case _ => Ssl.clientWithoutCertificateValidation()
      })))

    def withMaxRequestSize(size: StorageUnit): Client =
      configured(param.MaxRequestSize(size))

    def withMaxResponseSize(size: StorageUnit): Client =
      configured(param.MaxResponseSize(size))
  }

  val client = Client()

  def newClient(dest: Name, label: String): ServiceFactory[HttpRequest, HttpResponse] =
    client.newClient(dest, label)

  case class Server(
    stack: Stack[ServiceFactory[HttpRequest, HttpResponse]] = StackServer.newStack
      .replace(TraceInitializerFilter.role, new HttpServerTraceInitializer[HttpRequest, HttpResponse]),
    params: Stack.Params = StackServer.defaultParams
  ) extends StdStackServer[HttpRequest, HttpResponse, Server] {
    protected type In = Any
    protected type Out = Any

    protected def newListener(): Listener[Any, Any] = {
      val com.twitter.finagle.param.Label(label) = params[com.twitter.finagle.param.Label]
      val httpPipeline =
        param.applyToCodec(params, http.Http())
          .server(ServerCodecConfig(label, new SocketAddress{}))
          .pipelineFactory
      Netty3Listener(httpPipeline, params)
    }

    protected def newDispatcher(transport: Transport[In, Out],
        service: Service[HttpRequest, HttpResponse]) = {
      val dtab = DtabFilter.Netty
      new HttpServerDispatcher(
        new HttpTransport(transport), dtab andThen service)
    }

    protected def copy1(
      stack: Stack[ServiceFactory[HttpRequest, HttpResponse]] = this.stack,
      params: Stack.Params = this.params
    ): Server = copy(stack, params)

    def withTls(cfg: Netty3ListenerTLSConfig): Server =
      configured(Transport.TLSServerEngine(Some(cfg.newEngine)))

    def withMaxRequestSize(size: StorageUnit): Server =
      configured(param.MaxRequestSize(size))

    def withMaxResponseSize(size: StorageUnit): Server =
      configured(param.MaxResponseSize(size))
  }

  val server = Server()

  def serve(addr: SocketAddress, service: ServiceFactory[HttpRequest, HttpResponse]): ListeningServer =
    server.serve(addr, service)
}
