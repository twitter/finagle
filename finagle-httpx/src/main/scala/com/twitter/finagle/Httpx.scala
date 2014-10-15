package com.twitter.finagle

import com.twitter.conversions.storage._
import com.twitter.finagle.client._
import com.twitter.finagle.dispatch.SerialServerDispatcher
import com.twitter.finagle.httpx.codec.{HttpClientDispatcher, HttpServerDispatcher}
import com.twitter.finagle.httpx.filter.DtabFilter
import com.twitter.finagle.httpx.{
  HttpTransport, HttpServerTracingFilter, HttpClientTracingFilter, Request,
  Response
}
import com.twitter.finagle.netty3._
import com.twitter.finagle.server._
import com.twitter.finagle.ssl.Ssl
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Future, StorageUnit}
import java.net.{InetSocketAddress, SocketAddress}

/**
 * A rich client with a *very* basic URL fetcher. (It does not handle
 * redirects, does not have a cookie jar, etc.)
 */
trait HttpxRichClient { self: Client[Request, Response] =>
  def fetchUrl(url: String): Future[Response] = fetchUrl(new java.net.URL(url))
  def fetchUrl(url: java.net.URL): Future[Response] = {
    val addr = {
      val port = if (url.getPort < 0) url.getDefaultPort else url.getPort
      new InetSocketAddress(url.getHost, port)
    }
    val group = Group[SocketAddress](addr)
    val req = httpx.RequestBuilder().url(url).buildGet()
    val service = newClient(group).toService
    service(req) ensure {
      service.close()
    }
  }
}

/**
 * Http protocol support, including client and server.
 */
object Httpx extends Client[Request, Response] with HttpxRichClient
    with Server[Request, Response] {

  object param {
    case class MaxRequestSize(size: StorageUnit)
    implicit object MaxRequestSize extends Stack.Param[MaxRequestSize] {
      val default = MaxRequestSize(5.megabytes)
    }

    case class MaxResponseSize(size: StorageUnit)
    implicit object MaxResponseSize extends Stack.Param[MaxResponseSize] {
      val default = MaxResponseSize(5.megabytes)
    }

    case class Streaming(enabled: Boolean)
    implicit object Streaming extends Stack.Param[Streaming] {
      val default = Streaming(false)
    }

    private[Httpx] def applyToCodec(
      params: Stack.Params, codec: httpx.Http): httpx.RichHttp[Request] = {
      val richCodec = httpx.RichHttp[Request](
        codec
          .maxRequestSize(params[MaxRequestSize].size)
          .maxResponseSize(params[MaxResponseSize].size))
      applyToCodec(params, richCodec)
    }

    private[Httpx] def applyToCodec(
      params: Stack.Params, codec: httpx.RichHttp[Request]): httpx.RichHttp[Request] =
      codec.copy(aggregateChunks = !params[Streaming].enabled)
  }

  object Client {
    val stack: Stack[ServiceFactory[Request, Response]] =
      HttpClientTracingFilter.module[Request, Response] +: StackClient.newStack
  }

  case class Client(
    stack: Stack[ServiceFactory[Request, Response]] = Client.stack,
    params: Stack.Params = StackClient.defaultParams
  ) extends StdStackClient[Request, Response, Client] {
    protected type In = Any
    protected type Out = Any

    protected def newTransporter(): Transporter[Any, Any] = {
      val com.twitter.finagle.param.Label(label) = params[com.twitter.finagle.param.Label]
      val httpPipeline =
        param.applyToCodec(params, httpx.Http())
          .client(ClientCodecConfig(label))
          .pipelineFactory
      Netty3Transporter(httpPipeline, params)
    }

    protected def copy1(
      stack: Stack[ServiceFactory[Request, Response]] = this.stack,
      params: Stack.Params = this.params
    ): Client = copy(stack, params)

    protected def newDispatcher(transport: Transport[Any, Any]): Service[Request, Response] =
      new HttpClientDispatcher(transport)

    def withTls(cfg: Netty3TransporterTLSConfig): Client =
      configured((Transport.TLSClientEngine(Some(cfg.newEngine))))
        .configured(Transporter.TLSHostname(cfg.verifyHost))
        .transformed { stk => httpx.TlsFilter.module +: stk }

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

  def newClient(dest: Name, label: String): ServiceFactory[Request, Response] =
    client.newClient(dest, label)

  case class Server(
    stack: Stack[ServiceFactory[Request, Response]] = StackServer.newStack,
    params: Stack.Params = StackServer.defaultParams
  ) extends StdStackServer[Request, Response, Server] {
    protected type In = Any
    protected type Out = Any

    protected def newListener(): Listener[Any, Any] = {
      val com.twitter.finagle.param.Label(label) = params[com.twitter.finagle.param.Label]
      val httpPipeline =
        param.applyToCodec(params, httpx.Http())
          .server(ServerCodecConfig(label, new SocketAddress{}))
          .pipelineFactory
      Netty3Listener(httpPipeline, params)
    }

    protected def newDispatcher(transport: Transport[In, Out],
        service: Service[Request, Response]) = {
      val dtab = new DtabFilter.Finagle[Request]
      val tracingFilter = new HttpServerTracingFilter[Request, Response]("http")
      new HttpServerDispatcher(
        new HttpTransport(transport),
        tracingFilter andThen dtab andThen service
      )
    }

    protected def copy1(
      stack: Stack[ServiceFactory[Request, Response]] = this.stack,
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

  def serve(addr: SocketAddress, service: ServiceFactory[Request, Response]): ListeningServer =
    server.serve(addr, service)
}
