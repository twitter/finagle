package com.twitter.finagle

import com.twitter.finagle
import com.twitter.finagle.client._
import com.twitter.finagle.filter.NackAdmissionFilter
import com.twitter.finagle.http._
import com.twitter.finagle.http.codec.HttpServerDispatcher
import com.twitter.finagle.http.StreamTransport
import com.twitter.finagle.http.filter._
import com.twitter.finagle.http.param.ClientKerberosConfiguration
import com.twitter.finagle.http.param.ServerKerberosConfiguration
import com.twitter.finagle.http.service.HttpResponseClassifier
import com.twitter.finagle.http2.Http2Listener
import com.twitter.finagle.netty4.http.Netty4HttpListener
import com.twitter.finagle.netty4.http.Netty4ServerStreamTransport
import com.twitter.finagle.param.StandardStats
import com.twitter.finagle.server._
import com.twitter.finagle.service.TimeoutFilter.PreferDeadlineOverTimeout
import com.twitter.finagle.service.ResponseClassifier
import com.twitter.finagle.service.RetryBudget
import com.twitter.finagle.ssl.ApplicationProtocols
import com.twitter.finagle.stats.ExceptionStatsHandler
import com.twitter.finagle.stats.SourceRole
import com.twitter.finagle.stats.StandardStatsReceiver
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.tracing._
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.transport.TransportContext
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.FuturePool
import com.twitter.util.Monitor
import com.twitter.util.StorageUnit
import java.net.SocketAddress
import java.util.concurrent.ExecutorService

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
    service(req).respond { _ => service.close() }
  }
}

/**
 * HTTP/1.1 protocol support, including client and server.
 */
object Http extends Client[Request, Response] with HttpRichClient with Server[Request, Response] {

  /**
   * configure alternative http 1.1 implementations
   *
   * @param clientEndpointer client `Stackable[ServiceFactory]`
   * @param serverTransport server [[StreamTransport]] factory
   * @param listener [[Listener]] factory
   */
  private[finagle] final class HttpImpl private (
    private[finagle] val clientEndpointer: Stackable[ServiceFactory[Request, Response]],
    private[finagle] val serverTransport: Transport[Any, Any] => StreamTransport[Response, Request],
    private[finagle] val listener: Stack.Params => Listener[Any, Any, TransportContext],
    private[finagle] val implName: String) {

    def mk(): (HttpImpl, Stack.Param[HttpImpl]) = (this, HttpImpl.httpImplParam)
  }

  private[finagle] object HttpImpl {
    implicit val httpImplParam: Stack.Param[HttpImpl] = Stack.Param(Http11Impl)

    val Http11Impl: Http.HttpImpl = new Http.HttpImpl(
      ClientEndpointer.HttpEndpointer,
      new Netty4ServerStreamTransport(_),
      Netty4HttpListener,
      "Netty4"
    )

    val Http2Impl: Http.HttpImpl = new Http.HttpImpl(
      ClientEndpointer.Http2Endpointer,
      new Netty4ServerStreamTransport(_),
      Http2Listener.apply _,
      "Netty4"
    )
  }

  private val Http2Params: Stack.Params = Stack.Params.empty +
    HttpImpl.Http2Impl +
    com.twitter.finagle.param.ProtocolLibrary("http/2") +
    com.twitter.finagle.netty4.ssl.Alpn(ApplicationProtocols.Supported(Seq("h2", "http/1.1")))

  private val protocolLibrary = com.twitter.finagle.param.ProtocolLibrary("http")

  private[this] def treatServerErrorsAsFailures: Boolean = serverErrorsAsFailures()

  /** exposed for testing */
  private[finagle] val responseClassifierParam: finagle.param.ResponseClassifier = {
    def filtered[A, B](predicate: () => Boolean, pf: PartialFunction[A, B]): PartialFunction[A, B] =
      new PartialFunction[A, B] {
        def isDefinedAt(a: A): Boolean = predicate() && pf.isDefinedAt(a)
        def apply(a: A): B = pf(a)
      }

    val srvErrsAsFailures =
      filtered(() => treatServerErrorsAsFailures, HttpResponseClassifier.ServerErrorsAsFailures)

    val rc = ResponseClassifier.named("ToggledServerErrorsAsFailures") {
      srvErrsAsFailures.orElse(ResponseClassifier.Default)
    }

    finagle.param.ResponseClassifier(rc)
  }

  object Client {
    private val stack: Stack[ServiceFactory[Request, Response]] =
      StackClient.newStack
        .insertBefore(StackClient.Role.prepConn, ClientDtabContextFilter.module)
        .insertBefore(StackClient.Role.prepConn, ClientContextFilter.module)
        // We insert the ClientNackFilter close to the bottom of the stack to
        // eagerly transform the HTTP nack representation to a `Failure`.
        .insertBefore(StackClient.Role.prepConn, ClientNackFilter.module)
        // We add a DelayedRelease module at the bottom of the stack to ensure
        // that the pooling levels above don't discard an active session.
        .replace(StackClient.Role.prepConn, DelayedRelease.module(StackClient.Role.prepConn))
        // Since NackAdmissionFilter should operate on all requests sent over
        // the wire including retries, it must be below `Retries`. Since it
        // aggregates the status of the entire cluster, it must be above
        // `LoadBalancerFactory` (not part of the endpoint stack).
        .replace(
          StackClient.Role.prepFactory,
          NackAdmissionFilter.module[http.Request, http.Response]
        )
        // Ensure that FactoryToService doesn't release the connection to the layers
        // below when the response body hasn't been fully consumed.
        .replace(
          StackClient.Role.requestDraining,
          DelayedRelease.module(StackClient.Role.requestDraining))
        .replace(TraceInitializerFilter.role, new HttpClientTraceInitializer[Request, Response])
        .prepend(http.TlsFilter.module)
        // Because the payload filter also traces the sizes, it's important that we do so
        // after the tracing context is initialized.
        .insertAfter(
          TraceInitializerFilter.role,
          PayloadSizeFilter.module(PayloadSizeFilter.clientTraceKeyPrefix)
        )
        .replace(StackClient.Role.protoTracing, HttpTracingFilter.module)
        // The pooling strategy depends on whether we're using H2, and if so, what variant.
        // We use a custom module to isolate the selection logic.
        .replace(DefaultPool.Role, HttpPool)
        .prepend(
          new Stack.NoOpModule(http.filter.StatsFilter.role, http.filter.StatsFilter.description)
        )
        .insertAfter(http.filter.StatsFilter.role, StreamingStatsFilter.module)
        .prepend(KerberosAuthenticationFilter.clientModule)

    private def params: Stack.Params =
      StackClient.defaultParams +
        protocolLibrary +
        responseClassifierParam +
        PreferDeadlineOverTimeout(enabled = true)
  }

  case class Client(
    stack: Stack[ServiceFactory[Request, Response]] = Client.stack,
    params: Stack.Params = Client.params)
      extends EndpointerStackClient[Request, Response, Client]
      with finagle.param.WithSessionPool[Client]
      with finagle.param.WithDefaultLoadBalancer[Client]
      with Stack.Transformable[Client] {

    protected type In = Any
    protected type Out = Any
    protected type Context = TransportContext

    protected def endpointer: Stackable[ServiceFactory[Request, Response]] =
      params[HttpImpl].clientEndpointer

    protected def copy1(
      stack: Stack[ServiceFactory[Request, Response]] = this.stack,
      params: Stack.Params = this.params
    ): Client = copy(stack, params)

    def withTls(hostname: String): Client = withTransport.tls(hostname)

    def withTlsWithoutValidation: Client = withTransport.tlsWithoutValidation

    /**
     * Configures the sni hostname for SSL.
     *
     * @see [[https://docs.oracle.com/javase/8/docs/api/javax/net/ssl/SNIHostName.html Java's
     * SNIHostName]] for more details.
     */
    def withSni(hostname: String): Client = withTransport.sni(hostname)

    /**
     * For HTTP1*, configures the max size of headers
     * For HTTP2, sets the MAX_HEADER_LIST_SIZE setting which is the maximum
     * number of uncompressed bytes of header name/values.
     * These may be set independently via the .configured API.
     */
    def withMaxHeaderSize(size: StorageUnit): Client =
      this
        .configured(http.param.MaxHeaderSize(size))
        .configured(http2.param.MaxHeaderListSize(size))

    /**
     * Configures the maximum initial line length the client can receive from a server.
     */
    def withMaxInitialLineSize(size: StorageUnit): Client =
      configured(http.param.MaxInitialLineSize(size))

    /**
     * Configures the maximum response size that client can receive.
     */
    def withMaxResponseSize(size: StorageUnit): Client =
      configured(http.param.MaxResponseSize(size))

    /**
     * Streaming allows applications to work with HTTP messages that have large
     * (or infinite) content bodies.
     *
     * If `enabled` is set to `true`, the message content is available through a
     * [[com.twitter.io.Reader]], which gives the application a handle to the byte stream.
     *
     * If `enabled` is set to `false`, the entire message content is buffered up to
     * maximum allowed message size.
     */
    def withStreaming(enabled: Boolean): Client =
      configured(http.param.Streaming(enabled))

    /**
     * Streaming allows applications to work with HTTP messages that have large
     * (or infinite) content bodies.
     *
     * This method configures `fixedLengthStreamedAfter` limit, which effectively turns on
     * streaming (think `withStreaming(true)`). The `fixedLengthStreamedAfter`, however, disables
     * streaming for sufficiently small messages of known fixed length.
     *
     * If `Content-Length` of a message does not exceed `fixedLengthStreamedAfter` it is
     * buffered and its content is available through [[Request.content]] or
     * [[Request.contentString]].
     *
     * Messages without `Content-Length` header are always streamed regardless of their
     * actual content length and the `fixedLengthStreamedAfter` value.
     *
     * [[Response.isChunked]] should be used to determine whether a message is streamed
     * (`isChunked == true`) or buffered (`isChunked == false`).
     */
    def withStreaming(fixedLengthStreamedAfter: StorageUnit): Client =
      configured(http.param.Streaming(fixedLengthStreamedAfter))

    /**
     * Enables decompression of http content bodies.
     */
    def withDecompression(enabled: Boolean): Client =
      configured(http.param.Decompression(enabled))

    /**
     * Enable the collection of HTTP specific metrics. See [[http.filter.StatsFilter]].
     */
    def withHttpStats: Client =
      withStack(stack.replace(http.filter.StatsFilter.role, http.filter.StatsFilter.module))

    /**
     * Enable HTTP/2
     *
     * @note this will override whatever has been set in the toggle.
     */
    def withHttp2: Client =
      configuredParams(Http2Params)

    /**
     * Disable HTTP/2
     *
     * @note this will override whatever has been set in the toggle.
     */
    def withNoHttp2: Client =
      configured(HttpImpl.Http11Impl)

    /**
     * Enable kerberos client authentication for http requests
     */
    def withKerberos(clientKerberosConfiguration: ClientKerberosConfiguration): Client = configured(
      http.param.ClientKerberos(clientKerberosConfiguration))

    /**
     * Create a [[http.MethodBuilder]] for a given destination.
     *
     * @see [[https://twitter.github.io/finagle/guide/MethodBuilder.html user guide]]
     */
    def methodBuilder(dest: String): http.MethodBuilder =
      http.MethodBuilder.from(dest, this)

    /**
     * Create a [[http.MethodBuilder]] for a given destination.
     *
     * @see [[https://twitter.github.io/finagle/guide/MethodBuilder.html user guide]]
     */
    def methodBuilder(dest: Name): http.MethodBuilder =
      http.MethodBuilder.from(dest, this)

    // Java-friendly forwarders
    // See https://issues.scala-lang.org/browse/SI-8905
    override val withSessionPool: finagle.param.SessionPoolingParams[Client] =
      new finagle.param.SessionPoolingParams(this)
    override val withLoadBalancer: finagle.param.DefaultLoadBalancingParams[Client] =
      new finagle.param.DefaultLoadBalancingParams(this)
    override val withSessionQualifier: finagle.param.SessionQualificationParams[Client] =
      new finagle.param.SessionQualificationParams(this)
    override val withAdmissionControl: finagle.param.ClientAdmissionControlParams[Client] =
      new finagle.param.ClientAdmissionControlParams(this)
    override val withSession: finagle.param.ClientSessionParams[Client] =
      new finagle.param.ClientSessionParams(this)
    override val withTransport: finagle.param.ClientTransportParams[Client] =
      new finagle.param.ClientTransportParams(this)

    override def withResponseClassifier(
      responseClassifier: finagle.service.ResponseClassifier
    ): Client =
      super.withResponseClassifier(responseClassifier)
    override def withRetryBudget(budget: RetryBudget): Client = super.withRetryBudget(budget)
    override def withRetryBackoff(backoff: Backoff): Client =
      super.withRetryBackoff(backoff)
    override def withLabel(label: String): Client = super.withLabel(label)
    override def withStatsReceiver(statsReceiver: StatsReceiver): Client =
      super.withStatsReceiver(statsReceiver)
    override def withMonitor(monitor: Monitor): Client = super.withMonitor(monitor)
    override def withTracer(tracer: Tracer): Client = super.withTracer(tracer)
    override def withExceptionStatsHandler(exceptionStatsHandler: ExceptionStatsHandler): Client =
      super.withExceptionStatsHandler(exceptionStatsHandler)
    override def withRequestTimeout(timeout: Duration): Client = super.withRequestTimeout(timeout)

    override def withStack(stack: Stack[ServiceFactory[Request, Response]]): Client =
      super.withStack(stack)
    override def withStack(
      fn: Stack[ServiceFactory[Request, Response]] => Stack[ServiceFactory[Request, Response]]
    ): Client =
      super.withStack(fn)
    override def withExecutionOffloaded(executor: ExecutorService): Client =
      super.withExecutionOffloaded(executor)
    override def withExecutionOffloaded(pool: FuturePool): Client =
      super.withExecutionOffloaded(pool)
    override def configured[P](psp: (P, Stack.Param[P])): Client = super.configured(psp)
    override def configuredParams(newParams: Stack.Params): Client =
      super.configuredParams(newParams)
    override def filtered(filter: Filter[Request, Response, Request, Response]): Client =
      super.filtered(filter)

    private def superNewClient(dest: Name, label0: String): ServiceFactory[Request, Response] = {
      super.newClient(dest, label0)
    }
    override def newClient(dest: Name, label0: String): ServiceFactory[Request, Response] = {
      val client =
        if (params.contains[HttpImpl]) this
        else
          defaultClientProtocol() match {
            case Protocol.HTTP_2 => withHttp2
            case Protocol.HTTP_1_1 => withNoHttp2
          }

      client.superNewClient(dest, label0)
    }

    override def transformed(t: Stack.Transformer): Client =
      withStack(t(stack))
  }

  def client: Http.Client = Client()

  def newService(dest: Name, label: String): Service[Request, Response] =
    client.newService(dest, label)

  def newClient(dest: Name, label: String): ServiceFactory[Request, Response] =
    client.newClient(dest, label)

  object Server {
    private val stack: Stack[ServiceFactory[Request, Response]] =
      StackServer.newStack
      // Because the payload filter also traces the sizes, it's important that we do so
      // after the tracing context is initialized.
        .insertAfter(
          TraceInitializerFilter.role,
          PayloadSizeFilter.module(PayloadSizeFilter.serverTraceKeyPrefix)
        )
        .replace(StackServer.Role.protoTracing, HttpTracingFilter.module)
        .replace(TraceInitializerFilter.role, new HttpServerTraceInitializer[Request, Response])
        .replace(StackServer.Role.preparer, HttpNackFilter.module)
        .prepend(ServerDtabContextFilter.module)
        .prepend(
          new Stack.NoOpModule(http.filter.StatsFilter.role, http.filter.StatsFilter.description)
        )
        .prepend(KerberosAuthenticationFilter.serverModule)
        .insertAfter(http.filter.StatsFilter.role, StreamingStatsFilter.module)
        // the backup request module adds tracing annotations and as such must come
        // after trace initialization and deserialization of contexts.
        .insertAfter(TraceInitializerFilter.role, ServerContextFilter.module)
        .insertAfter(
          ServerContextFilter.role,
          BackupRequest.traceAnnotationModule[Request, Response])

    private def params: Stack.Params = StackServer.defaultParams +
      protocolLibrary +
      responseClassifierParam +
      StandardStats(
        stats.StatsAndClassifier(
          new StandardStatsReceiver(SourceRole.Server, protocolLibrary.name),
          HttpResponseClassifier.ServerErrorsAsFailures)) +
      PreferDeadlineOverTimeout(enabled = true)
  }

  case class Server(
    stack: Stack[ServiceFactory[Request, Response]] = Server.stack,
    params: Stack.Params = Server.params)
      extends StdStackServer[Request, Response, Server] {

    protected type In = Any
    protected type Out = Any
    protected type Context = TransportContext

    private[this] val dispatcherStats =
      params[finagle.param.Stats].statsReceiver.scope("dispatch")

    protected def newListener(): Listener[Any, Any, TransportContext] = {
      params[HttpImpl].listener(params)
    }

    private[this] def newStreamTransport(
      transport: Transport[Any, Any]
    ): StreamTransport[Response, Request] =
      new HttpTransport(params[HttpImpl].serverTransport(transport))

    protected def newDispatcher(
      transport: Transport[In, Out] { type Context <: Server.this.Context },
      service: Service[Request, Response]
    ): HttpServerDispatcher =
      new HttpServerDispatcher(newStreamTransport(transport), service, dispatcherStats)

    protected def copy1(
      stack: Stack[ServiceFactory[Request, Response]] = this.stack,
      params: Stack.Params = this.params
    ): Server = copy(stack, params)

    /**
     * For HTTP1*, configures the max size of headers
     * For HTTP2, sets the MAX_HEADER_LIST_SIZE setting which is the maximum
     * number of uncompressed bytes of header name/values.
     * These may be set independently via the .configured API.
     */
    def withMaxHeaderSize(size: StorageUnit): Server =
      this
        .configured(http.param.MaxHeaderSize(size))
        .configured(http2.param.MaxHeaderListSize(size))

    /**
     * Configures the maximum request size this server can receive.
     */
    def withMaxRequestSize(size: StorageUnit): Server =
      configured(http.param.MaxRequestSize(size))

    /**
     * Streaming allows applications to work with HTTP messages that have large
     * (or infinite) content bodies.
     *
     * If `enabled` is set to `true`, the message content is available through a
     * [[com.twitter.io.Reader]], which gives the application a handle to the byte stream.
     *
     * If `enabled` is set to `false`, the entire message content is buffered up to
     * maximum allowed message size.
     */
    def withStreaming(enabled: Boolean): Server =
      configured(http.param.Streaming(enabled))

    /**
     * Streaming allows applications to work with HTTP messages that have large
     * (or infinite) content bodies.
     *
     * This method configures `fixedLengthStreamedAfter` limit, which effectively turns on
     * streaming (think `withStreaming(true)`). The `fixedLengthStreamedAfter`, however, disables
     * streaming for sufficiently small messages of known fixed length.
     *
     * If `Content-Length` of a message does not exceed `fixedLengthStreamedAfter` it is
     * buffered and its content is available through [[Request.content]] or
     * [[Request.contentString]].
     *
     * Messages without `Content-Length` header are always streamed regardless of their
     * actual content length and the `fixedLengthStreamedAfter` value.
     *
     * [[Request.isChunked]] should be used to determine whether a message is streamed
     * (`isChunked == true`) or buffered (`isChunked == false`).
     */
    def withStreaming(fixedLengthStreamedAfter: StorageUnit): Server =
      configured(http.param.Streaming(fixedLengthStreamedAfter))

    /**
     * Enables decompression of http content bodies.
     */
    def withDecompression(enabled: Boolean): Server =
      configured(http.param.Decompression(enabled))

    /**
     * The compression level to use. If passed the default value (-1) then it will use
     * [[com.twitter.finagle.http.codec.TextualContentCompressor TextualContentCompressor]]
     * which will compress text-like content-types with the default compression level (6).
     * Otherwise, use the Netty `HttpContentCompressor` for all content-types with specified
     * compression level.
     */
    def withCompressionLevel(level: Int): Server =
      configured(http.param.CompressionLevel(level))

    /**
     * Configures the maximum initial http line length the server is
     * willing to accept.
     */
    def withMaxInitialLineSize(size: StorageUnit): Server =
      configured(http.param.MaxInitialLineSize(size))

    /**
     * Enable the collection of HTTP specific metrics. See [[http.filter.StatsFilter]].
     */
    def withHttpStats: Server =
      withStack(stack.replace(http.filter.StatsFilter.role, http.filter.StatsFilter.module))

    /**
     * Enable HTTP/2
     *
     * @note this will override whatever has been set in the toggle.
     */
    def withHttp2: Server =
      configuredParams(Http2Params)

    /**
     * Disable HTTP/2
     *
     * @note this will override whatever has been set in the toggle.
     */
    def withNoHttp2: Server =
      configured(HttpImpl.Http11Impl)

    /**
     * Enable kerberos server authentication for http requests
     */
    def withKerberos(serverKerberosConfiguration: ServerKerberosConfiguration): Server = configured(
      http.param.ServerKerberos(serverKerberosConfiguration))

    /**
     * By default finagle-http automatically sends 100-CONTINUE responses to inbound
     * requests which set the 'Expect: 100-Continue' header. Streaming servers will
     * always return 100-CONTINUE. Non-streaming servers will compare the
     * content-length header to the configured limit (see: `withMaxRequestSize`)
     * and send either a 100-CONTINUE or 413-REQUEST ENTITY TOO LARGE as
     * appropriate. This method disables those automatic responses.
     *
     * @note Servers operating as proxies should disable automatic responses in
     *       order to allow origin servers to determine whether the expectation
     *       can be met.
     * @note Disabling automatic continues is only supported in
     *       [[com.twitter.finagle.Http.HttpImpl.Http11Impl]] servers.
     */
    def withNoAutomaticContinue: Server =
      configured(http.param.AutomaticContinue(false))

    // Java-friendly forwarders
    // See https://issues.scala-lang.org/browse/SI-8905
    override val withAdmissionControl: finagle.param.ServerAdmissionControlParams[Server] =
      new finagle.param.ServerAdmissionControlParams(this)
    override val withTransport: finagle.param.ServerTransportParams[Server] =
      new finagle.param.ServerTransportParams(this)
    override val withSession: finagle.param.ServerSessionParams[Server] =
      new finagle.param.ServerSessionParams(this)

    override def withResponseClassifier(
      responseClassifier: finagle.service.ResponseClassifier
    ): Server =
      super.withResponseClassifier(responseClassifier)
    override def withLabel(label: String): Server = super.withLabel(label)
    override def withStatsReceiver(statsReceiver: StatsReceiver): Server =
      super.withStatsReceiver(statsReceiver)
    override def withMonitor(monitor: Monitor): Server = super.withMonitor(monitor)
    override def withTracer(tracer: Tracer): Server = super.withTracer(tracer)
    override def withExceptionStatsHandler(exceptionStatsHandler: ExceptionStatsHandler): Server =
      super.withExceptionStatsHandler(exceptionStatsHandler)
    override def withRequestTimeout(timeout: Duration): Server = super.withRequestTimeout(timeout)

    override def withStack(stack: Stack[ServiceFactory[Request, Response]]): Server =
      super.withStack(stack)

    override def withStack(
      fn: Stack[ServiceFactory[Request, Response]] => Stack[ServiceFactory[Request, Response]]
    ): Server =
      super.withStack(fn)
    override def withExecutionOffloaded(executor: ExecutorService): Server =
      super.withExecutionOffloaded(executor)
    override def withExecutionOffloaded(pool: FuturePool): Server =
      super.withExecutionOffloaded(pool)
    override def configured[P](psp: (P, Stack.Param[P])): Server = super.configured(psp)
    override def configuredParams(newParams: Stack.Params): Server =
      super.configuredParams(newParams)

    protected def superServe(
      addr: SocketAddress,
      factory: ServiceFactory[Request, Response]
    ): ListeningServer = {
      super.serve(addr, factory)
    }
    override def serve(
      addr: SocketAddress,
      factory: ServiceFactory[Request, Response]
    ): ListeningServer = {
      val server =
        if (params.contains[HttpImpl]) this
        else
          defaultServerProtocol() match {
            case Protocol.HTTP_2 => withHttp2
            case Protocol.HTTP_1_1 => withNoHttp2
          }

      server.superServe(addr, factory)
    }
  }

  def server: Http.Server = Server()

  def serve(addr: SocketAddress, service: ServiceFactory[Request, Response]): ListeningServer =
    server.serve(addr, service)
}
