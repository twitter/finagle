package com.twitter.finagle

import com.twitter.conversions.storage._
import com.twitter.finagle.client._
import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.filter.PayloadSizeFilter
import com.twitter.finagle.http.{HttpClientTraceInitializer, HttpServerTraceInitializer, HttpTransport, Request, Response}
import com.twitter.finagle.http.codec.{HttpClientDispatcher, HttpServerDispatcher}
import com.twitter.finagle.http.filter.{ClientContextFilter, DtabFilter, HttpNackFilter, ServerContextFilter}
import com.twitter.finagle.netty3._
import com.twitter.finagle.param.{Monitor => _, ResponseClassifier => _, ExceptionStatsHandler => _, Tracer => _, _}
import com.twitter.finagle.server._
import com.twitter.finagle.service.RetryBudget
import com.twitter.finagle.stats.{ExceptionStatsHandler, StatsReceiver}
import com.twitter.finagle.tracing._
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Duration, Future, StorageUnit, Monitor}
import java.net.SocketAddress
import org.jboss.netty.channel.Channel

/**
 * A rich client with a *very* basic URL fetcher. (It does not handle
 * redirects, does not have a cookie jar, etc.)
 */
trait HttpRichClient { self: Client[Request, Response] =>
  def fetchUrl(url: String): Future[Response] = fetchUrl(new java.net.URL(url))
  def fetchUrl(url: java.net.URL): Future[Response] = {
    val addr = {
      val port = if (url.getPort < 0) url.getDefaultPort else url.getPort
      Address(url.getHost, port)
    }
    val req = http.RequestBuilder().url(url).buildGet()
    val service = newService(Name.bound(addr), "")
    service(req) ensure {
      service.close()
    }
  }
}

/**
 * Http protocol support, including client and server.
 */
object Http extends Client[Request, Response] with HttpRichClient
    with Server[Request, Response] {

  object param {
    case class MaxRequestSize(size: StorageUnit) {
      require(size < 2.gigabytes,
        s"MaxRequestSize should be less than 2 Gb, but was $size")
    }
    implicit object MaxRequestSize extends Stack.Param[MaxRequestSize] {
      val default = MaxRequestSize(5.megabytes)
    }

    case class MaxResponseSize(size: StorageUnit) {
      require(size < 2.gigabytes,
        s"MaxResponseSize should be less than 2 Gb, but was $size")
    }
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

    private[Http] def applyToCodec(
      params: Stack.Params, codec: http.Http): http.Http =
        codec
          .maxRequestSize(params[MaxRequestSize].size)
          .maxResponseSize(params[MaxResponseSize].size)
          .streaming(params[Streaming].enabled)
          .decompressionEnabled(params[Decompression].enabled)
          .compressionLevel(params[CompressionLevel].level)
  }

  // Only record payload sizes when streaming is disabled.
  private[this] val nonChunkedPayloadSize: Stackable[ServiceFactory[Request, Response]] =
    new Stack.Module2[param.Streaming, Stats, ServiceFactory[Request, Response]] {
      override def role: Stack.Role = PayloadSizeFilter.Role
      override def description: String = PayloadSizeFilter.Description

      override def make(
        streaming: param.Streaming,
        stats: Stats,
        next: ServiceFactory[Request, Response]
      ): ServiceFactory[Request, Response] = {
        if (!streaming.enabled)
          new PayloadSizeFilter[Request, Response](
            stats.statsReceiver, _.content.length, _.content.length).andThen(next)
        else next
      }
    }

  object Client {
    val stack: Stack[ServiceFactory[Request, Response]] =
      StackClient.newStack
        .replace(TraceInitializerFilter.role, new HttpClientTraceInitializer[Request, Response])
        .prepend(http.TlsFilter.module)
        .prepend(nonChunkedPayloadSize)
  }

  case class Client(
      stack: Stack[ServiceFactory[Request, Response]] = Client.stack,
      params: Stack.Params = StackClient.defaultParams + ProtocolLibrary("http"))
    extends StdStackClient[Request, Response, Client]
    with WithSessionPool[Client]
    with WithDefaultLoadBalancer[Client] {

    protected type In = Any
    protected type Out = Any

    protected def newTransporter(): Transporter[Any, Any] = {
      val com.twitter.finagle.param.Label(label) = params[com.twitter.finagle.param.Label]
      val codec = param.applyToCodec(params, http.Http())
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

    protected def newDispatcher(transport: Transport[Any, Any]): Service[Request, Response] = {
      val dispatcher = new HttpClientDispatcher(
        transport,
        params[Stats].statsReceiver.scope(GenSerialClientDispatcher.StatsScope)
      )

      new ClientContextFilter[Request, Response].andThen(dispatcher)
    }

    def withTls(cfg: Netty3TransporterTLSConfig): Client =
      configured(Transport.TLSClientEngine(Some(cfg.newEngine)))
      .configured(Transporter.TLSHostname(cfg.verifyHost))

    def withTls(hostname: String): Client = withTransport.tls(hostname)

    def withTlsWithoutValidation: Client = withTransport.tlsWithoutValidation

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

    // Java-friendly forwarders
    // See https://issues.scala-lang.org/browse/SI-8905
    override val withSessionPool: SessionPoolingParams[Client] =
      new SessionPoolingParams(this)
    override val withLoadBalancer: DefaultLoadBalancingParams[Client] =
      new DefaultLoadBalancingParams(this)
    override val withSessionQualifier: SessionQualificationParams[Client] =
      new SessionQualificationParams(this)
    override val withAdmissionControl: ClientAdmissionControlParams[Client] =
      new ClientAdmissionControlParams(this)
    override val withSession: SessionParams[Client] =
      new SessionParams(this)
    override val withTransport: ClientTransportParams[Client] =
      new ClientTransportParams(this)

    override def withResponseClassifier(responseClassifier: service.ResponseClassifier): Client =
     super.withResponseClassifier(responseClassifier)
    override def withRetryBudget(budget: RetryBudget): Client = super.withRetryBudget(budget)
    override def withRetryBackoff(backoff: Stream[Duration]): Client = super.withRetryBackoff(backoff)
    override def withLabel(label: String): Client = super.withLabel(label)
    override def withStatsReceiver(statsReceiver: StatsReceiver): Client =
      super.withStatsReceiver(statsReceiver)
    override def withMonitor(monitor: Monitor): Client = super.withMonitor(monitor)
    override def withTracer(tracer: Tracer): Client = super.withTracer(tracer)
    override def withExceptionStatsHandler(exceptionStatsHandler: ExceptionStatsHandler): Client =
      super.withExceptionStatsHandler(exceptionStatsHandler)
    override def withRequestTimeout(timeout: Duration): Client = super.withRequestTimeout(timeout)

    override def configured[P](psp: (P, Stack.Param[P])): Client = super.configured(psp)
    override def filtered(filter: Filter[Request, Response, Request, Response]): Client =
      super.filtered(filter)
  }

  val client: Http.Client = Client()

  def newService(dest: Name, label: String): Service[Request, Response] =
    client.newService(dest, label)

  def newClient(dest: Name, label: String): ServiceFactory[Request, Response] =
    client.newClient(dest, label)

  object Server {
    val stack: Stack[ServiceFactory[Request, Response]] =
      StackServer.newStack
        .replace(TraceInitializerFilter.role, new HttpServerTraceInitializer[Request, Response])
        .replace(StackServer.Role.preparer, HttpNackFilter.module)
        .prepend(nonChunkedPayloadSize)
  }

  case class Server(
      stack: Stack[ServiceFactory[Request, Response]] = Server.stack,
      params: Stack.Params = StackServer.defaultParams + ProtocolLibrary("http"))
    extends StdStackServer[Request, Response, Server] {

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
        service: Service[Request, Response]) = {
      val dtab = new DtabFilter.Finagle[Request]
      val context = new ServerContextFilter[Request, Response]
      val Stats(stats) = params[Stats]

      val endpoint = dtab.andThen(context).andThen(service)

      new HttpServerDispatcher(new HttpTransport(transport), endpoint, stats.scope("dispatch"))
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

    // Java-friendly forwarders
    // See https://issues.scala-lang.org/browse/SI-8905
    override val withAdmissionControl: ServerAdmissionControlParams[Server] =
      new ServerAdmissionControlParams(this)
    override val withTransport: ServerTransportParams[Server] =
      new ServerTransportParams[Server](this)

    override def withResponseClassifier(responseClassifier: service.ResponseClassifier): Server =
      super.withResponseClassifier(responseClassifier)
    override def withLabel(label: String): Server = super.withLabel(label)
    override def withStatsReceiver(statsReceiver: StatsReceiver): Server =
      super.withStatsReceiver(statsReceiver)
    override def withMonitor(monitor: Monitor): Server = super.withMonitor(monitor)
    override def withTracer(tracer: Tracer): Server = super.withTracer(tracer)
    override def withExceptionStatsHandler(exceptionStatsHandler: ExceptionStatsHandler): Server =
      super.withExceptionStatsHandler(exceptionStatsHandler)
    override def withRequestTimeout(timeout: Duration): Server = super.withRequestTimeout(timeout)

    override def configured[P](psp: (P, Stack.Param[P])): Server = super.configured(psp)
  }

  val server: Http.Server = Server()

  def serve(addr: SocketAddress, service: ServiceFactory[Request, Response]): ListeningServer =
    server.serve(addr, service)
}
