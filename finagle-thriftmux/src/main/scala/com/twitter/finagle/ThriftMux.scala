package com.twitter.finagle

import com.twitter.finagle.client.{ClientRegistry, ExceptionRemoteInfoFactory, StackBasedClient, StackClient}
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.context.RemoteInfo.Upstream
import com.twitter.finagle.mux.{OpportunisticTlsParams, Request, Response}
import com.twitter.finagle.mux.exp.pushsession.MuxPush
import com.twitter.finagle.mux.lease.exp.Lessor
import com.twitter.finagle.mux.transport.{MuxContext, OpportunisticTls}
import com.twitter.finagle.param.{ExceptionStatsHandler => _, Monitor => _, ResponseClassifier => _, Tracer => _, _}
import com.twitter.finagle.server.{Listener, ServerInfo, StackBasedServer, StackServer, StdStackServer}
import com.twitter.finagle.service._
import com.twitter.finagle.stats.{
  ClientStatsReceiver,
  ExceptionStatsHandler,
  ServerStatsReceiver,
  StatsReceiver
}
import com.twitter.finagle.thrift._
import com.twitter.finagle.thriftmux.Toggles
import com.twitter.finagle.thriftmux.pushsession.MuxDowngradingNegotiator
import com.twitter.finagle.thriftmux.service.ThriftMuxResponseClassifier
import com.twitter.finagle.tracing.Tracer
import com.twitter.finagle.transport.{StatsTransport, Transport}
import com.twitter.io.Buf
import com.twitter.util._
import java.net.SocketAddress
import org.apache.thrift.protocol.TProtocolFactory
import org.apache.thrift.TException

/**
 * The `ThriftMux` object is both a `com.twitter.finagle.Client` and a
 * `com.twitter.finagle.Server` for the Thrift protocol served over
 * [[com.twitter.finagle.mux]]. Rich interfaces are provided to adhere to those
 * generated from a [[https://thrift.apache.org/docs/idl Thrift IDL]] by
 * [[https://twitter.github.io/scrooge/ Scrooge]] or
 * [[https://github.com/mariusaeriksen/thrift-finagle thrift-finagle]].
 *
 * == Clients ==
 *
 * Clients can be created directly from an interface generated from
 * a Thrift IDL:
 *
 * For example, this IDL:
 *
 * {{{
 * service TestService {
 *   string query(1: string x)
 * }
 * }}}
 *
 * compiled with Scrooge, generates the interface
 * `TestService.FutureIface`. This is then passed
 * into `ThriftMux.Client.newIface`:
 *
 * {{{
 * ThriftMux.client.newIface[TestService.FutureIface](
 *   addr, classOf[TestService.FutureIface])
 * }}}
 *
 * However note that the Scala compiler can insert the latter
 * `Class` for us, for which another variant of `newIface` is
 * provided:
 *
 * {{{
 * ThriftMux.client.newIface[TestService.FutureIface](addr)
 * }}}
 *
 * In Java, we need to provide the class object:
 *
 * {{{
 * TestService.FutureIface client =
 *   ThriftMux.client.newIface(addr, TestService.FutureIface.class);
 * }}}
 *
 * == Servers ==
 *
 * Servers are also simple to expose:
 *
 * `TestService.FutureIface` must be implemented and passed
 * into `serveIface`:
 *
 * {{{
 * // An echo service
 * ThriftMux.server.serveIface(":*", new TestService.FutureIface {
 *   def query(x: String): Future[String] = Future.value(x)
 * })
 * }}}
 *
 * This object does not expose any configuration options. Both clients and servers
 * are instantiated with sane defaults. Clients are labeled with the "clnt/thrift"
 * prefix and servers with "srv/thrift". If you'd like more configuration, see the
 * [[https://twitter.github.io/finagle/guide/Configuration.html#clients-and-servers
 * configuration documentation]].
 */
object ThriftMux
  extends Client[ThriftClientRequest, Array[Byte]]
    with Server[Array[Byte], Array[Byte]] {

  /**
   * Base [[com.twitter.finagle.Stack]] for ThriftMux clients.
   */
  val BaseClientStack: Stack[ServiceFactory[mux.Request, mux.Response]] =
    (ThriftMuxUtil.protocolRecorder +: Mux.client.stack)

  /**
   * Base [[com.twitter.finagle.Stack]] for ThriftMux servers.
   */
  val BaseServerStack: Stack[ServiceFactory[mux.Request, mux.Response]] =
  // NOTE: ideally this would not use the `prepConn` role, but it's conveniently
  // located in the right location of the stack and is defaulted to a no-op.
  // We would like this located anywhere before the StatsFilter so that success
  // and failure can be measured properly before converting the exceptions into
  // byte arrays. see CSL-1351
    ThriftMuxUtil.protocolRecorder +:
      Mux.server.stack.replace(StackServer.Role.preparer, Server.ExnHandler)

  /**
   * Base [[com.twitter.finagle.Stack.Params]] for ThriftMux servers.
   */
  val BaseServerParams: Stack.Params = Mux.Server.params + ProtocolLibrary("thriftmux")

  object Client extends ThriftClient {

    def apply(): Client =
      new Client()
        .withLabel("thrift")
        .withStatsReceiver(ClientStatsReceiver)

    private[finagle] def pushMuxer: StackClient[mux.Request, mux.Response] =
      MuxPush.client
        .copy(stack = BaseClientStack)
        .configured(ProtocolLibrary("thriftmux"))

    private[finagle] def standardMuxer: StackClient[mux.Request, mux.Response] =
      Mux.client
        .copy(stack = BaseClientStack)
        .configured(ProtocolLibrary("thriftmux"))
  }

  /**
   * A ThriftMux `com.twitter.finagle.Client`.
   *
   * @see [[https://twitter.github.io/finagle/guide/Configuration.html#clients-and-servers Configuration]] documentation
   * @see [[https://twitter.github.io/finagle/guide/Protocols.html#thrift Thrift]] documentation
   * @see [[https://twitter.github.io/finagle/guide/Protocols.html#mux Mux]] documentation
   */
  case class Client(muxer: StackClient[mux.Request, mux.Response] = Client.pushMuxer)
    extends StackBasedClient[ThriftClientRequest, Array[Byte]]
      with Stack.Parameterized[Client]
      with Stack.Transformable[Client]
      with CommonParams[Client]
      with ClientParams[Client]
      with WithClientTransport[Client]
      with WithClientAdmissionControl[Client]
      with WithClientSession[Client]
      with WithSessionQualifier[Client]
      with WithDefaultLoadBalancer[Client]
      with ThriftRichClient
      with OpportunisticTlsParams[Client] {

    def stack: Stack[ServiceFactory[mux.Request, mux.Response]] =
      muxer.stack

    def params: Stack.Params = muxer.params

    protected lazy val Label(defaultClientName) = params[Label]

    protected val clientParam: RichClientParam = RichClientParam(
      protocolFactory = params[Thrift.param.ProtocolFactory].protocolFactory,
      maxThriftBufferSize = params[Thrift.param.MaxReusableBufferSize].maxReusableBufferSize,
      clientStats = params[Stats].statsReceiver,
      responseClassifier = params[com.twitter.finagle.param.ResponseClassifier].responseClassifier,
      perEndpointStats = params[Thrift.param.PerEndpointStats].enabled
    )

    @deprecated("Use clientParam.protocolFactory", "2017-08-16")
    protected def protocolFactory: TProtocolFactory = clientParam.protocolFactory

    @deprecated("Use clientParam.clientStats", "2017-08-16")
    override protected def stats: StatsReceiver = clientParam.clientStats

    def withParams(ps: Stack.Params): Client =
      copy(muxer = muxer.withParams(ps))

    def transformed(t: Stack.Transformer): Client =
      copy(muxer = muxer.transformed(t))

    /**
     * Produce a [[com.twitter.finagle.ThriftMux.Client]] using the provided
     * client ID.
     */
    def withClientId(clientId: ClientId): Client =
      configured(Thrift.param.ClientId(Some(clientId)))

    /**
     * Produce a [[com.twitter.finagle.ThriftMux.Client]] using the provided
     * protocolFactory.
     */
    def withProtocolFactory(pf: TProtocolFactory): Client =
      configured(Thrift.param.ProtocolFactory(pf))

    /**
     * Produce a [[com.twitter.finagle.ThriftMux.Client]] using the provided stack.
     */
    def withStack(stack: Stack[ServiceFactory[mux.Request, mux.Response]]): Client =
      this.copy(muxer = muxer.withStack(stack))

    /**
     * Prepends `filter` to the top of the client. That is, after materializing
     * the client (newClient/newService) `filter` will be the first element which
     * requests flow through. This is a familiar chaining combinator for filters.
     */
    def filtered(filter: Filter[mux.Request, mux.Response, mux.Request, mux.Response]): Client = {
      val role = Stack.Role(filter.getClass.getSimpleName)
      val stackable = Filter.canStackFromFac.toStackable(role, filter)
      withStack(stackable +: stack)
    }

    /**
     * Produce a [[com.twitter.finagle.ThriftMux.Client]] with the specified max
     * size of the reusable buffer for thrift responses. If this size
     * is exceeded, the buffer is not reused and a new buffer is
     * allocated for the next thrift response.
     * The default max size is 16Kb.
     *
     * @param size Max size of the reusable buffer for thrift responses in bytes.
     */
    def withMaxReusableBufferSize(size: Int): Client =
      configured(Thrift.param.MaxReusableBufferSize(size))

    /**
     * Produce a [[com.twitter.finagle.ThriftMux.Client]] with per-endpoint stats filters
     */
    def withPerEndpointStats: Client =
      configured(Thrift.param.PerEndpointStats(true))

    private[this] def clientId: Option[ClientId] = params[Thrift.param.ClientId].clientId

    private[this] object ThriftMuxToMux
      extends Filter[ThriftClientRequest, Array[Byte], mux.Request, mux.Response] {

      private val extractResponseBytesFn = (response: mux.Response) => {
        val responseCtx = Contexts.local.getOrElse(Headers.Response.Key, EmptyResponseHeadersFn)
        responseCtx.set(response.contexts)
        Buf.ByteArray.Owned.extract(response.body)
      }

      private val EmptyRequestHeadersFn: () => Headers.Values = () => Headers.Request.newValues
      private val EmptyResponseHeadersFn: () => Headers.Values = () => Headers.Response.newValues

      def apply(
        req: ThriftClientRequest,
        service: Service[mux.Request, mux.Response]
      ): Future[Array[Byte]] = {
        if (req.oneway)
          return Future.exception(
            new UnsupportedOperationException("ThriftMux does not support one-way messages")
          )

        // We set ClientId a bit early, because ThriftMux relies on that broadcast
        // context to be set when dispatching.
        ExceptionRemoteInfoFactory.letUpstream(Upstream.addr, ClientId.current.map(_.name)) {
          ClientId.let(clientId) {
            val requestCtx = Contexts.local.getOrElse(Headers.Request.Key, EmptyRequestHeadersFn)
            // TODO set the Path here.
            val muxRequest = mux.Request(Path.empty, requestCtx.values, Buf.ByteArray.Owned(req.message))
            service(muxRequest).map(extractResponseBytesFn)
          }
        }
      }
    }

    private[this] def deserializingClassifier: StackClient[mux.Request, mux.Response] = {
      // Note: what type of deserializer used is important if none is specified
      // so that we keep the prior behavior of Thrift exceptions
      // being counted as a success. Otherwise, even using the default
      // ResponseClassifier would then see that response as a `Throw` and thus
      // a failure. So, when none is specified, a "deserializing-only"
      // classifier is used to make when deserialization happens in the stack
      // uniform whether or not a `ResponseClassifier` is wired up.
      val classifier = if (params.contains[param.ResponseClassifier]) {
        ThriftMuxResponseClassifier.usingDeserializeCtx(
          params[param.ResponseClassifier].responseClassifier
        )
      } else {
        ThriftMuxResponseClassifier.DeserializeCtxOnly
      }
      muxer.configured(param.ResponseClassifier(classifier))
    }

    def newService(dest: Name, label: String): Service[ThriftClientRequest, Array[Byte]] = {
      clientId.foreach(id => ClientRegistry.export(params, "ClientId", id.name))
      ThriftMuxToMux.andThen(deserializingClassifier.newService(dest, label))
    }

    def newClient(dest: Name, label: String): ServiceFactory[ThriftClientRequest, Array[Byte]] = {
      clientId.foreach(id => ClientRegistry.export(params, "ClientId", id.name))
      ThriftMuxToMux.andThen(deserializingClassifier.newClient(dest, label))
    }

    /**
     * Create a [[thriftmux.MethodBuilder]] for a given destination.
     *
     * @see [[https://twitter.github.io/finagle/guide/MethodBuilder.html user guide]]
     */
    def methodBuilder(dest: String): thriftmux.MethodBuilder =
      thriftmux.MethodBuilder.from(dest, this)

    /**
     * Create a [[thriftmux.MethodBuilder]] for a given destination.
     *
     * @see [[https://twitter.github.io/finagle/guide/MethodBuilder.html user guide]]
     */
    def methodBuilder(dest: Name): thriftmux.MethodBuilder =
      thriftmux.MethodBuilder.from(dest, this)

    // Java-friendly forwarders
    // See https://issues.scala-lang.org/browse/SI-8905
    override val withTransport: ClientTransportParams[Client] =
    new ClientTransportParams(this)
    override val withSession: ClientSessionParams[Client] =
      new ClientSessionParams(this)
    override val withLoadBalancer: DefaultLoadBalancingParams[Client] =
      new DefaultLoadBalancingParams(this)
    override val withSessionQualifier: SessionQualificationParams[Client] =
      new SessionQualificationParams(this)
    override val withAdmissionControl: ClientAdmissionControlParams[Client] =
      new ClientAdmissionControlParams(this)

    override def withLabel(label: String): Client = super.withLabel(label)
    override def withStatsReceiver(statsReceiver: StatsReceiver): Client =
      super.withStatsReceiver(statsReceiver)
    override def withMonitor(monitor: Monitor): Client = super.withMonitor(monitor)
    override def withTracer(tracer: Tracer): Client = super.withTracer(tracer)
    override def withExceptionStatsHandler(exceptionStatsHandler: ExceptionStatsHandler): Client =
      super.withExceptionStatsHandler(exceptionStatsHandler)
    override def withRequestTimeout(timeout: Duration): Client = super.withRequestTimeout(timeout)
    override def withResponseClassifier(responseClassifier: ResponseClassifier): Client =
      super.withResponseClassifier(responseClassifier)
    override def withRetryBudget(budget: RetryBudget): Client = super.withRetryBudget(budget)
    override def withRetryBackoff(backoff: Stream[Duration]): Client =
      super.withRetryBackoff(backoff)

    override def configured[P](psp: (P, Stack.Param[P])): Client = super.configured(psp)
  }

  def client: ThriftMux.Client = Client()

  protected val Thrift.param.ProtocolFactory(protocolFactory) =
    client.params[Thrift.param.ProtocolFactory]

  def newClient(
    dest: Name,
    label: String
  ): ServiceFactory[ThriftClientRequest, Array[Byte]] =
    client.newClient(dest, label)

  def newService(
    dest: Name,
    label: String
  ): Service[ThriftClientRequest, Array[Byte]] =
    client.newService(dest, label)

  /**
   * A server for the Thrift protocol served over [[com.twitter.finagle.mux]].
   * ThriftMuxServer is backwards-compatible with Thrift clients that use the
   * framed transport and binary protocol. It switches to the backward-compatible
   * mode when the first request is not recognized as a valid Mux message but can
   * be successfully handled by the underlying Thrift service. Since a Thrift
   * message that is encoded with the binary protocol starts with a header value of
   * 0x800100xx, Mux does not confuse it with a valid Mux message (0x80 = -128 is
   * an invalid Mux message type) and the server can reliably detect the non-Mux
   * Thrift client and switch to the backwards-compatible mode.
   *
   * Note that the server is also compatible with non-Mux finagle-thrift clients.
   * It correctly responds to the protocol up-negotiation request and passes the
   * tracing information embedded in the thrift requests to Mux (which has native
   * tracing support).
   */
  case class ServerMuxer(
    stack: Stack[ServiceFactory[mux.Request, mux.Response]] = BaseServerStack,
    params: Stack.Params = BaseServerParams
  ) extends StdStackServer[mux.Request, mux.Response, ServerMuxer] {

    protected type In = Buf
    protected type Out = Buf
    protected type Context = MuxContext

    private[this] val statsReceiver = params[Stats].statsReceiver


    override def serve(
      addr: SocketAddress,
      factory: ServiceFactory[Request, Response]
    ): ListeningServer = {
      // // We want to fail fast if the server's OppTls configuration is invalid
      Mux.Server.validateTlsParamConsistency(params)
      super.serve(addr, factory)
    }

    protected def copy1(
      stack: Stack[ServiceFactory[mux.Request, mux.Response]] = this.stack,
      params: Stack.Params = this.params
    ): ServerMuxer = copy(stack, params)

    protected def newListener(): Listener[In, Out, Context] =
      params[Mux.param.MuxImpl].listener(params)

    // we cache tlsHeaders here because it's hard to propagate a `let` to the
    // server's dispatcher in tests
    private[this] val cachedTlsHeaders = Mux.param.MuxImpl.tlsHeaders

    protected def newDispatcher(
      transport: Transport[In, Out] { type Context <: ServerMuxer.this.Context },
      service: Service[mux.Request, mux.Response]
    ): Closable = {
      val Lessor.Param(lessor) = params[Lessor.Param]
      val Mux.param.MaxFrameSize(frameSize) = params[Mux.param.MaxFrameSize]
      val muxStatsReceiver = statsReceiver.scope("mux")
      val param.ExceptionStatsHandler(excRecorder) = params[param.ExceptionStatsHandler]
      val param.Tracer(tracer) = params[param.Tracer]
      val Thrift.param.ProtocolFactory(pf) = params[Thrift.param.ProtocolFactory]
      val Mux.param.OppTls(level) = params[Mux.param.OppTls]
      val upgrades = muxStatsReceiver.counter("tls", "upgrade", "success")

      val thriftEmulator = thriftmux.ThriftEmulator(transport, pf, statsReceiver.scope("thriftmux"))

      val negotiatedTrans = mux.Handshake.server(
        trans = thriftEmulator,
        version = Mux.LatestVersion,
        headers = Mux.Server.headers(_, frameSize, if (cachedTlsHeaders) level else None),
        negotiate = Mux.negotiate(
          frameSize,
          muxStatsReceiver,
          level.getOrElse(OpportunisticTls.Off),
          transport.context.turnOnTls _,
          upgrades
        )
      )

      val statsTrans =
        new StatsTransport(negotiatedTrans, excRecorder, muxStatsReceiver.scope("transport"))

      mux.ServerDispatcher.newRequestResponse(statsTrans, service, lessor, tracer, muxStatsReceiver)
    }
  }

  @deprecated("Use Server.defaultMuxer instead", "2018-02-01")
  def serverMuxer: StackServer[mux.Request, mux.Response] = Server.defaultMuxer

  object Server {

    private[finagle] val UsePushMuxServerToggleName =
      "com.twitter.finagle.thriftmux.UsePushMuxServer"
    private[this] val usePushMuxToggle = Toggles(UsePushMuxServerToggleName)
    private[this] def UsePushMuxServer: Boolean = usePushMuxToggle(ServerInfo().id.hashCode)

    /** The default underlying muxer for ThriftMux servers */
    def defaultMuxer: StackServer[mux.Request, mux.Response] =
      if(UsePushMuxServer) pushMuxer else standardMuxer

    private[finagle] def pushMuxer: StackServer[mux.Request, mux.Response] = {
      MuxPush.server
        .copy(
          stack = BaseServerStack,
          params = BaseServerParams,
          sessionFactory = MuxDowngradingNegotiator.build(_, _, _, _))
    }

    private[finagle] def standardMuxer: StackServer[mux.Request, mux.Response] = ServerMuxer()

    private val MuxToArrayFilter =
      new Filter[mux.Request, mux.Response, Array[Byte], Array[Byte]] {
        private[this] val responseBytesToMuxResponseFn = (responseBytes: Array[Byte]) => {
          mux.Response(
            ctxts = Contexts.local(Headers.Response.Key).values,
            buf = Buf.ByteArray.Owned(responseBytes))
        }

        def apply(
          request: mux.Request,
          service: Service[Array[Byte], Array[Byte]]
        ): Future[mux.Response] = {
          val reqBytes = Buf.ByteArray.Owned.extract(request.body)
          Contexts.local.let(
            Headers.Request.Key, Headers.Values(request.contexts),
            Headers.Response.Key, Headers.Response.newValues
          ) {
            service(reqBytes).map(responseBytesToMuxResponseFn)
          }
        }
      }

    // Convert unhandled exceptions to TApplicationExceptions, but pass
    // com.twitter.finagle.FailureFlags to mux for transmission.
    private[this] class ExnFilter(protocolFactory: TProtocolFactory)
      extends SimpleFilter[mux.Request, mux.Response] {
      def apply(
        request: mux.Request,
        service: Service[mux.Request, mux.Response]
      ): Future[mux.Response] =
        service(request).rescue {
          case f: FailureFlags[_] => Future.exception(f)
          case e if !e.isInstanceOf[TException] =>
            val msg =
              UncaughtAppExceptionFilter.writeExceptionMessage(request.body, e, protocolFactory)
            Future.value(mux.Response(Nil, msg))
        }
    }

    private[ThriftMux] val ExnHandler =
      new Stack.Module1[Thrift.param.ProtocolFactory, ServiceFactory[mux.Request, mux.Response]] {
        val role = Stack.Role("appExceptionHandling")
        val description = "Translates uncaught application exceptions into Thrift messages"
        def make(
          _pf: Thrift.param.ProtocolFactory,
          next: ServiceFactory[mux.Request, mux.Response]
        ): ServiceFactory[mux.Request, mux.Response] = {
          val Thrift.param.ProtocolFactory(pf) = _pf
          val exnFilter = new ExnFilter(pf)
          exnFilter.andThen(next)
        }
      }
  }

  /**
   * A ThriftMux `com.twitter.finagle.Server`.
   *
   * @see [[https://twitter.github.io/finagle/guide/Configuration.html#clients-and-servers Configuration]] documentation
   * @see [[https://twitter.github.io/finagle/guide/Protocols.html#thrift Thrift]] documentation
   * @see [[https://twitter.github.io/finagle/guide/Protocols.html#mux Mux]] documentation
   */
  final case class Server(muxer: StackServer[mux.Request, mux.Response] = Server.defaultMuxer)
    extends StackBasedServer[Array[Byte], Array[Byte]]
      with ThriftRichServer
      with Stack.Parameterized[Server]
      with CommonParams[Server]
      with WithServerTransport[Server]
      with WithServerSession[Server]
      with WithServerAdmissionControl[Server]
      with OpportunisticTlsParams[Server] {

    import Server.MuxToArrayFilter

    def stack: Stack[ServiceFactory[mux.Request, mux.Response]] =
      muxer.stack

    protected val serverParam: RichServerParam = RichServerParam(
      protocolFactory = params[Thrift.param.ProtocolFactory].protocolFactory,
      serviceName = params[Label].label,
      maxThriftBufferSize = params[Thrift.param.MaxReusableBufferSize].maxReusableBufferSize,
      serverStats = params[Stats].statsReceiver,
      perEndpointStats = params[Thrift.param.PerEndpointStats].enabled
    )

    @deprecated("Use serverParam.serviceName", "2017-08-16")
    override protected def serverLabel: String = serverParam.serviceName

    @deprecated("Use serverParam.serverStats", "2017-08-16")
    override protected def serverStats: StatsReceiver = serverParam.serverStats

    def params: Stack.Params = muxer.params

    @deprecated("Use serverParam.protocolFactory", "2017-08-16")
    protected def protocolFactory: TProtocolFactory = serverParam.protocolFactory

    @deprecated("Use serverParam.maxThriftBufferSize", "2017-08-16")
    override protected def maxThriftBufferSize: Int = serverParam.maxThriftBufferSize

    /**
     * Produce a [[com.twitter.finagle.ThriftMux.Server]] using the provided
     * `TProtocolFactory`.
     */
    def withProtocolFactory(pf: TProtocolFactory): Server =
      configured(Thrift.param.ProtocolFactory(pf))

    /**
     * Produce a [[com.twitter.finagle.ThriftMux.Server]] using the provided stack.
     */
    def withStack(stack: Stack[ServiceFactory[mux.Request, mux.Response]]): Server =
      this.copy(muxer = muxer.withStack(stack))

    /**
     * Prepends `filter` to the top of the server. That is, after materializing
     * the server (newService) `filter` will be the first element which requests
     * flow through. This is a familiar chaining combinator for filters.
     */
    def filtered(filter: Filter[mux.Request, mux.Response, mux.Request, mux.Response]): Server = {
      val role = Stack.Role(filter.getClass.getSimpleName)
      val stackable = Filter.canStackFromFac.toStackable(role, filter)
      withStack(stackable +: stack)
    }

    /**
     * Produce a [[com.twitter.finagle.ThriftMux.Server]] with the specified max
     * size of the reusable buffer for thrift responses. If this size
     * is exceeded, the buffer is not reused and a new buffer is
     * allocated for the next thrift response.
     * The default max size is 16Kb.
     *
     * @param size Max size of the reusable buffer for thrift responses in bytes.
     */
    def withMaxReusableBufferSize(size: Int): Server =
      configured(Thrift.param.MaxReusableBufferSize(size))

    /**
     * Produce a [[com.twitter.finagle.ThriftMux.Server]] with per-endpoint stats filters
     */
    def withPerEndpointStats: Server =
      configured(Thrift.param.PerEndpointStats(true))

    def withParams(ps: Stack.Params): Server =
      copy(muxer = muxer.withParams(ps))

    def serve(
      addr: SocketAddress,
      factory: ServiceFactory[Array[Byte], Array[Byte]]
    ): ListeningServer = {
      muxer.serve(addr, MuxToArrayFilter.andThen(factory))
    }

    // Java-friendly forwarders
    // See https://issues.scala-lang.org/browse/SI-8905
    override val withTransport: ServerTransportParams[Server] =
    new ServerTransportParams(this)
    override val withSession: SessionParams[Server] =
      new SessionParams(this)
    override val withAdmissionControl: ServerAdmissionControlParams[Server] =
      new ServerAdmissionControlParams(this)

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

  def server: ThriftMux.Server = Server()
    .configured(Label("thrift"))
    .configured(Stats(ServerStatsReceiver))

  def serve(
    addr: SocketAddress,
    factory: ServiceFactory[Array[Byte], Array[Byte]]
  ): ListeningServer =
    server.serve(addr, factory)
}
