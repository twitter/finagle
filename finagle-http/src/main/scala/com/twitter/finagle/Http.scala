package com.twitter.finagle

import com.twitter.conversions.storage._
import com.twitter.finagle.Http.param.HttpImpl
import com.twitter.finagle.client._
import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.filter.PayloadSizeFilter
import com.twitter.finagle.http._
import com.twitter.finagle.http.codec.{HttpClientDispatcher, HttpServerDispatcher}
import com.twitter.finagle.http.exp.StreamTransport
import com.twitter.finagle.http.filter.{ClientContextFilter, HttpNackFilter, ServerContextFilter}
import com.twitter.finagle.http.netty.{Netty3ClientStreamTransport, Netty3HttpListener, Netty3HttpTransporter, Netty3ServerStreamTransport}
import com.twitter.finagle.http.service.HttpResponseClassifier
import com.twitter.finagle.netty3._
import com.twitter.finagle.param.{ResponseClassifier => ResponseClassifierParam, ExceptionStatsHandler => _, Monitor => _, Tracer => _, _}
import com.twitter.finagle.server._
import com.twitter.finagle.service.{ResponseClassifier, RetryBudget}
import com.twitter.finagle.stats.{ExceptionStatsHandler, StatsReceiver}
import com.twitter.finagle.tracing._
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Duration, Future, Monitor, StorageUnit}
import java.net.SocketAddress

/**
 * A rich HTTP/1.1 client with a *very* basic URL fetcher. (It does not handle
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
 * HTTP/1.1 protocol support, including client and server.
 */
object Http extends Client[Request, Response] with HttpRichClient
    with Server[Request, Response] {

  object param {
    /**
     * configure alternative http 1.1 implementations
     *
     * @param clientTransport client [[StreamTransport]] factory
     * @param serverTransport server [[StreamTransport]] factory
     * @param transporter [[Transporter]] factory
     * @param listener [[Listener]] factory
     */
    case class HttpImpl(
      clientTransport: Transport[Any, Any] => StreamTransport[Request, Response],
      serverTransport: Transport[Any, Any] => StreamTransport[Response, Request],
      transporter: Stack.Params => Transporter[Any, Any],
      listener: Stack.Params => Listener[Any, Any]) {

      def mk(): (HttpImpl, Stack.Param[HttpImpl]) =
        (this, HttpImpl)
    }

    implicit object HttpImpl extends Stack.Param[HttpImpl] {
      val default = Netty3Impl
    }

    private[finagle] val Netty3Impl: HttpImpl = HttpImpl(
      new Netty3ClientStreamTransport(_),
      new Netty3ServerStreamTransport(_),
      Netty3HttpTransporter,
      Netty3HttpListener
    )

    /**
     * when streaming, the maximum size of http chunks.
     */
    case class MaxChunkSize(size: StorageUnit)
    implicit object MaxChunkSize extends Stack.Param[MaxChunkSize] {
      val default = MaxChunkSize(8.kilobytes)
    }

    /**
     * the maximum size of all headers.
     */
    case class MaxHeaderSize(size: StorageUnit)
    implicit object MaxHeaderSize extends Stack.Param[MaxHeaderSize] {
      val default = MaxHeaderSize(8.kilobytes)
    }

    /**
     * the maximum size of the initial line.
     */
    case class MaxInitialLineSize(size: StorageUnit)
    implicit object MaxInitialLineSize extends Stack.Param[MaxInitialLineSize] {
      val default = MaxInitialLineSize(4.kilobytes)
    }

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

  }

  private val protocolLibrary = ProtocolLibrary("http")

  private[finagle] val ServerErrorsAsFailuresToggleId =
    "com.twitter.finagle.http.serverErrorsAsFailures"

  private[this] val serverErrorsAsFailuresToggle =
    http.Toggles("com.twitter.finagle.http.serverErrorsAsFailures")

  private[this] def treatServerErrorsAsFailures: Boolean =
    serverErrorsAsFailuresToggle(ServerInfo().id.hashCode)

  private val responseClassifierParam: ResponseClassifierParam = {
    val rc: ResponseClassifier =
      ResponseClassifier.named("ToggledServerErrorsAsFailures") { case reqRep =>
        if (treatServerErrorsAsFailures)
          HttpResponseClassifier.ServerErrorsAsFailures(reqRep)
        else
          ResponseClassifier.Default(reqRep)
      }
    ResponseClassifierParam(rc)
  }

  // Only record payload sizes when streaming is disabled.
  private[finagle] val nonChunkedPayloadSize: Stackable[ServiceFactory[Request, Response]] =
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
        .insertBefore(StackClient.Role.prepConn, ClientContextFilter.module)
        .replace(StackClient.Role.prepConn, DelayedRelease.module)
        .replace(StackClient.Role.prepFactory, DelayedRelease.module)
        .replace(TraceInitializerFilter.role, new HttpClientTraceInitializer[Request, Response])
        .prepend(http.TlsFilter.module)
        .prepend(nonChunkedPayloadSize)
        .prepend(new Stack.NoOpModule(http.filter.StatsFilter.role, http.filter.StatsFilter.description))

    private def params: Stack.Params =
      StackClient.defaultParams +
        protocolLibrary +
        responseClassifierParam
  }

  case class Client(
      stack: Stack[ServiceFactory[Request, Response]] = Client.stack,
      params: Stack.Params = Client.params)
    extends StdStackClient[Request, Response, Client]
    with WithSessionPool[Client]
    with WithDefaultLoadBalancer[Client] {

    protected type In = Any
    protected type Out = Any

    protected def newStreamTransport(
      transport: Transport[Any, Any]
    ): StreamTransport[Request, Response] =
      new HttpTransport(params[HttpImpl].clientTransport(transport))

    protected def newTransporter(): Transporter[Any, Any] = {
      params[param.HttpImpl].transporter(params)
    }

    protected def copy1(
      stack: Stack[ServiceFactory[Request, Response]] = this.stack,
      params: Stack.Params = this.params
    ): Client = copy(stack, params)

    protected def newDispatcher(transport: Transport[Any, Any]): Service[Request, Response] =
      new HttpClientDispatcher(
        newStreamTransport(transport),
        params[Stats].statsReceiver.scope(GenSerialClientDispatcher.StatsScope)
      )

    def withTls(cfg: Netty3TransporterTLSConfig): Client =
      configured(Transport.TLSClientEngine(Some(cfg.newEngine)))
      .configured(Transporter.TLSHostname(cfg.verifyHost))

    def withTls(hostname: String): Client = withTransport.tls(hostname)

    def withTlsWithoutValidation: Client = withTransport.tlsWithoutValidation

    def withMaxHeaderSize(size: StorageUnit): Client =
      configured(param.MaxHeaderSize(size))

    /**
     * Configures the maximum initial line length the client can
     * receive from a server.
     */
    def withMaxInitialLineSize(size: StorageUnit): Client =
      configured(param.MaxInitialLineSize(size))

    /**
     * Configures the maximum request size that the client can send.
     */
    def withMaxRequestSize(size: StorageUnit): Client =
      configured(param.MaxRequestSize(size))

    /**
     * Configures the maximum response size that client can receive.
     */
    def withMaxResponseSize(size: StorageUnit): Client =
      configured(param.MaxResponseSize(size))

    /**
     * Streaming allows applications to work with HTTP messages that have large
     * (or infinite) content bodies. When this set to `true`, the message content is
     * available through a [[com.twitter.io.Reader]], which gives the application a
     * handle to the byte stream. If `false`, the entire message content is buffered
     * into a [[com.twitter.io.Buf]].
     */
    def withStreaming(enabled: Boolean): Client =
      configured(param.Streaming(enabled))

    /**
     * Enables decompression of http content bodies.
     */
    def withDecompression(enabled: Boolean): Client =
      configured(param.Decompression(enabled))

    /**
     * The compression level to use. If passed the default value (-1) then it will use
     * [[com.twitter.finagle.http.codec.TextualContentCompressor TextualContentCompressor]]
     * which will compress text-like content-types with the default compression level (6).
     * Otherwise, use [[org.jboss.netty.handler.codec.http.HttpContentCompressor HttpContentCompressor]]
     * for all content-types with specified compression level.
     */

    def withCompressionLevel(level: Int): Client =
      configured(param.CompressionLevel(level))


    /**
     * Enable the collection of HTTP specific metrics. See [[http.filter.StatsFilter]].
     */
    def withHttpStats: Client =
      withStack(stack.replace(http.filter.StatsFilter.role, http.filter.StatsFilter.module))

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
    override val withSession: ClientSessionParams[Client] =
      new ClientSessionParams(this)
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
        .prepend(ServerContextFilter.module)
        .prepend(new Stack.NoOpModule(http.filter.StatsFilter.role, http.filter.StatsFilter.description))

    private def params: Stack.Params =
      StackServer.defaultParams +
        protocolLibrary +
        responseClassifierParam
  }

  case class Server(
      stack: Stack[ServiceFactory[Request, Response]] = Server.stack,
      params: Stack.Params = Server.params)
    extends StdStackServer[Request, Response, Server] {

    protected type In = Any
    protected type Out = Any

    protected def newListener(): Listener[Any, Any] = {
      params[param.HttpImpl].listener(params)
    }

    protected def newStreamTransport(
      transport: Transport[Any, Any]
    ): StreamTransport[Response, Request] =
      new HttpTransport(params[HttpImpl].serverTransport(transport))

    protected def newDispatcher(
      transport: Transport[In, Out],
      service: Service[Request, Response]
    ): HttpServerDispatcher = {
      val Stats(stats) = params[Stats]
      new HttpServerDispatcher(
        newStreamTransport(transport),
        service,
        stats.scope("dispatch"))
    }

    protected def copy1(
      stack: Stack[ServiceFactory[Request, Response]] = this.stack,
      params: Stack.Params = this.params
    ): Server = copy(stack, params)

    def withTls(cfg: Netty3ListenerTLSConfig): Server =
      configured(Transport.TLSServerEngine(Some(cfg.newEngine)))

    /**
     * Configures the maximum request size this server can receive.
     */
    def withMaxRequestSize(size: StorageUnit): Server =
      configured(param.MaxRequestSize(size))

    /**
     * Configures the maximum response size this server can send.
     */
    def withMaxResponseSize(size: StorageUnit): Server =
      configured(param.MaxResponseSize(size))

    /**
     * Streaming allows applications to work with HTTP messages that have large
     * (or infinite) content bodies. When this set to `true`, the message content is
     * available through a [[com.twitter.io.Reader]], which gives the application a
     * handle to the byte stream. If `false`, the entire message content is buffered
     * into a [[com.twitter.io.Buf]].
     */
    def withStreaming(enabled: Boolean): Server =
      configured(param.Streaming(enabled))

    /**
     * Enables decompression of http content bodies.
     */
    def withDecompression(enabled: Boolean): Server =
      configured(param.Decompression(enabled))

    /**
     * The compression level to use. If passed the default value (-1) then it will use
     * [[com.twitter.finagle.http.codec.TextualContentCompressor TextualContentCompressor]]
     * which will compress text-like content-types with the default compression level (6).
     * Otherwise, use [[org.jboss.netty.handler.codec.http.HttpContentCompressor HttpContentCompressor]]
     * for all content-types with specified compression level.
     */
    def withCompressionLevel(level: Int): Server =
      configured(param.CompressionLevel(level))

    /**
     * Configures the maximum initial http line length the server is
     * willing to accept.
     */
    def withMaxInitialLineSize(size: StorageUnit): Server =
      configured(param.MaxInitialLineSize(size))

    /**
     * Enable the collection of HTTP specific metrics. See [[http.filter.StatsFilter]].
     */
    def withHttpStats: Server =
      withStack(stack.replace(http.filter.StatsFilter.role, http.filter.StatsFilter.module))

    // Java-friendly forwarders
    // See https://issues.scala-lang.org/browse/SI-8905
    override val withAdmissionControl: ServerAdmissionControlParams[Server] =
      new ServerAdmissionControlParams(this)
    override val withTransport: ServerTransportParams[Server] =
      new ServerTransportParams[Server](this)
    override val withSession: SessionParams[Server] =
      new SessionParams(this)

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
