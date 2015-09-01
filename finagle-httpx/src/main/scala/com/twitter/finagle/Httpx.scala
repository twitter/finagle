package com.twitter.finagle

import com.twitter.conversions.storage._
import com.twitter.finagle.client._
import com.twitter.finagle.httpx.{HttpClientTraceInitializer, HttpServerTraceInitializer, HttpTransport, Request, Response}
import com.twitter.finagle.httpx.codec.{HttpClientDispatcher, HttpServerDispatcher}
import com.twitter.finagle.httpx.filter.{DtabFilter, HttpNackFilter}
import com.twitter.finagle.netty3._
import com.twitter.finagle.param.{ProtocolLibrary, Stats}
import com.twitter.finagle.server._
import com.twitter.finagle.ssl.Ssl
import com.twitter.finagle.tracing._
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Future, StorageUnit}
import java.net.{InetSocketAddress, SocketAddress}
import org.jboss.netty.channel.Channel

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
    val req = httpx.RequestBuilder().url(url).buildGet()
    val service = newService(Name.bound(addr), "")
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

    case class Decompression(enabled: Boolean)
    implicit object Decompression extends Stack.Param[Decompression] {
      val default = Decompression(enabled = true)
    }

    case class CompressionLevel(level: Int)
    implicit object CompressionLevel extends Stack.Param[CompressionLevel] {
      val default = CompressionLevel(-1)
    }

    private[Httpx] def applyToCodec(
      params: Stack.Params, codec: httpx.Http): httpx.Http =
        codec
          .maxRequestSize(params[MaxRequestSize].size)
          .maxResponseSize(params[MaxResponseSize].size)
          .streaming(params[Streaming].enabled)
          .decompressionEnabled(params[Decompression].enabled)
          .compressionLevel(params[CompressionLevel].level)
  }

  object Client {
    val stack: Stack[ServiceFactory[Request, Response]] =
      StackClient.newStack
        .replace(TraceInitializerFilter.role, new HttpClientTraceInitializer[Request, Response])
  }

  case class Client(
    stack: Stack[ServiceFactory[Request, Response]] = Client.stack,
    params: Stack.Params = StackClient.defaultParams + ProtocolLibrary("httpx")
  ) extends StdStackClient[Request, Response, Client] {
    protected type In = Any
    protected type Out = Any

    protected def newTransporter(): Transporter[Any, Any] = {
      val com.twitter.finagle.param.Label(label) = params[com.twitter.finagle.param.Label]
      val codec = param.applyToCodec(params, httpx.Http())
        .client(ClientCodecConfig(label))
      val Stats(stats) = params[Stats]
      val newTransport = (ch: Channel) => codec.newClientTransport(ch, stats)
      Netty3Transporter(
        codec.pipelineFactory,
        params + Netty3Transporter.TransportFactory(newTransport))
    }

    protected def copy1(
      stack: Stack[ServiceFactory[Request, Response]] = this.stack,
      params: Stack.Params = this.params
    ): Client = copy(stack, params)

    protected def newDispatcher(transport: Transport[Any, Any]): Service[Request, Response] =
      new HttpClientDispatcher(transport)

    def withTls(cfg: Netty3TransporterTLSConfig): Client =
      configured(Transport.TLSClientEngine(Some(cfg.newEngine)))
        .configured(Transporter.TLSHostname(cfg.verifyHost))
        .transformed { stk => httpx.TlsFilter.module +: stk }

    def withTls(hostname: String): Client =
      withTls(new Netty3TransporterTLSConfig({
        case inet: InetSocketAddress => Ssl.client(hostname, inet.getPort)
        case _ => Ssl.client()
      }, Some(hostname)))

    def withTlsWithoutValidation(): Client =
      configured(Transport.TLSClientEngine(Some({
        case inet: InetSocketAddress => Ssl.clientWithoutCertificateValidation(inet.getHostName, inet.getPort)
        case _ => Ssl.clientWithoutCertificateValidation()
      })))

    def withMaxRequestSize(size: StorageUnit): Client =
      configured(param.MaxRequestSize(size))

    def withMaxResponseSize(size: StorageUnit): Client =
      configured(param.MaxResponseSize(size))

    def withStreaming(enabled: Boolean): Client =
      configured(param.Streaming(enabled))

    def withDecompression(enabled: Boolean): Client =
      configured(param.Decompression(enabled))

    def withCompressionLevel(level: Int): Client =
      configured(param.CompressionLevel(level))
  }

  val client = Client()

  def newService(dest: Name, label: String): Service[Request, Response] =
    client.newService(dest, label)

  def newClient(dest: Name, label: String): ServiceFactory[Request, Response] =
    client.newClient(dest, label)

  object Server {
    val stack: Stack[ServiceFactory[Request, Response]] =
      StackServer.newStack
        .replace(TraceInitializerFilter.role, new HttpServerTraceInitializer[Request, Response])
        .replace(
          StackServer.Role.preparer,
          (next: ServiceFactory[Request, Response]) => (new HttpNackFilter).andThen(next))
  }

  case class Server(
    stack: Stack[ServiceFactory[Request, Response]] = Server.stack,
    params: Stack.Params = StackServer.defaultParams + ProtocolLibrary("httpx")
  ) extends StdStackServer[Request, Response, Server] {
    protected type In = Any
    protected type Out = Any

    // This override allows java callers to use this method, working around https://issues.scala-lang.org/browse/SI-8905
    override def configured[P](psp: (P, Stack.Param[P])): StackServer[Request, Response] = super.configured[P](psp)

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
      val Stats(stats) = params[Stats]

      new HttpServerDispatcher(new HttpTransport(transport), dtab andThen service, stats.scope("dispatch"))
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

    def withStreaming(enabled: Boolean): Server =
      configured(param.Streaming(enabled))

    def withDecompression(enabled: Boolean): Server =
      configured(param.Decompression(enabled))

    def withCompressionLevel(level: Int): Server =
      configured(param.CompressionLevel(level))
  }

  val server = Server()

  def serve(addr: SocketAddress, service: ServiceFactory[Request, Response]): ListeningServer =
    server.serve(addr, service)
}
