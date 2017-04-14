package com.twitter.finagle

import com.twitter.finagle.client.{ClientRegistry, StackBasedClient, StackClient}
import com.twitter.finagle.mux.lease.exp.Lessor
import com.twitter.finagle.param.{ExceptionStatsHandler => _, Monitor => _, ResponseClassifier => _, Tracer => _, _}
import com.twitter.finagle.server.{Listener, StackBasedServer, StackServer, StdStackServer}
import com.twitter.finagle.service._
import com.twitter.finagle.stats.{ClientStatsReceiver, ExceptionStatsHandler, ServerStatsReceiver, StatsReceiver}
import com.twitter.finagle.thrift.{ClientId, ThriftClientRequest, UncaughtAppExceptionFilter}
import com.twitter.finagle.thriftmux.service.ThriftMuxResponseClassifier
import com.twitter.finagle.tracing.{Trace, Tracer}
import com.twitter.finagle.transport.{StatsTransport, Transport}
import com.twitter.io.Buf
import com.twitter.util._
import java.net.SocketAddress
import org.apache.thrift.protocol.TProtocolFactory
import org.apache.thrift.TException
import org.apache.thrift.transport.TMemoryInputTransport
import scala.util.control.NonFatal

/**
 * The `ThriftMux` object is both a `com.twitter.finagle.Client` and a
 * `com.twitter.finagle.Server` for the Thrift protocol served over
 * [[com.twitter.finagle.mux]]. Rich interfaces are provided to adhere to those
 * generated from a [[http://thrift.apache.org/docs/idl/ Thrift IDL]] by
 * [[https://twitter.github.io/scrooge/ Scrooge]] or
 * [[https://github.com/mariusaeriksen/thrift-0.5.0-finagle thrift-finagle]].
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
  with Server[Array[Byte], Array[Byte]]
{
  /**
   * Base [[com.twitter.finagle.Stack]] for ThriftMux clients.
   */
  private[twitter] val BaseClientStack: Stack[ServiceFactory[mux.Request, mux.Response]] =
    (ThriftMuxUtil.protocolRecorder +: Mux.client.stack)
      .replace(StackClient.Role.protoTracing, ClientRpcTracing)

  /**
   * Base [[com.twitter.finagle.Stack]] for ThriftMux servers.
   */
  private[twitter] val BaseServerStack: Stack[ServiceFactory[mux.Request, mux.Response]] =
    // NOTE: ideally this would not use the `prepConn` role, but it's conveniently
    // located in the right location of the stack and is defaulted to a no-op.
    // We would like this located anywhere before the StatsFilter so that success
    // and failure can be measured properly before converting the exceptions into
    // byte arrays. see CSL-1351
    ThriftMuxUtil.protocolRecorder +:
      Mux.server.stack.replace(StackServer.Role.preparer, Server.ExnHandler)

  private[this] def recordRpc(buffer: Array[Byte]): Unit = try {
    val inputTransport = new TMemoryInputTransport(buffer)
    val iprot = protocolFactory.getProtocol(inputTransport)
    val msg = iprot.readMessageBegin()
    Trace.recordRpc(msg.name)
  } catch {
    case NonFatal(_) =>
  }

  private object ClientRpcTracing extends Mux.ClientProtoTracing {
    private[this] val rpcTracer = new SimpleFilter[mux.Request, mux.Response] {
      def apply(
        request: mux.Request,
        svc: Service[mux.Request, mux.Response]
      ): Future[mux.Response] = {
        // we're reasonably sure that this filter sits just after the ThriftClientRequest's
        // message array is wrapped by a ChannelBuffer
        recordRpc(Buf.ByteArray.Owned.extract(request.body))
        svc(request)
      }
    }

    override def make(
      next: ServiceFactory[mux.Request, mux.Response]
    ): ServiceFactory[mux.Request, mux.Response] =
      rpcTracer.andThen(super.make(next))
  }

  /**
   * A ThriftMux `com.twitter.finagle.Client`.
   *
   * @see [[https://twitter.github.io/finagle/guide/Configuration.html#clients-and-servers Configuration]] documentation
   * @see [[https://twitter.github.io/finagle/guide/Protocols.html#thrift Thrift]] documentation
   * @see [[https://twitter.github.io/finagle/guide/Protocols.html#mux Mux]] documentation
   */
  case class Client(
      muxer: StackClient[mux.Request, mux.Response] = Mux.client.copy(stack = BaseClientStack)
        .configured(ProtocolLibrary("thriftmux")))
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
    with ThriftRichClient {

    def stack: Stack[ServiceFactory[mux.Request, mux.Response]] =
      muxer.stack

    def params: Stack.Params = muxer.params

    protected lazy val Label(defaultClientName) = params[Label]

    override protected lazy val Stats(stats) = params[Stats]

    protected val Thrift.param.ProtocolFactory(protocolFactory) =
      params[Thrift.param.ProtocolFactory]

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

    private[this] def clientId: Option[ClientId] = params[Thrift.param.ClientId].clientId

    private[this] object ThriftMuxToMux extends Filter[ThriftClientRequest, Array[Byte], mux.Request, mux.Response] {
      def apply(req: ThriftClientRequest, service: Service[mux.Request, mux.Response]): Future[Array[Byte]] = {
        if (req.oneway) return Future.exception(
          new Exception("ThriftMux does not support one-way messages"))

        // We do a dance here to ensure that the proper ClientId is set when
        // `service` is applied because Mux relies on
        // com.twitter.finagle.thrift.ClientIdContext to propagate ClientIds.
        ClientId.let(clientId) {
          // TODO set the Path here.
          val muxreq = mux.Request(Path.empty, Buf.ByteArray.Owned(req.message))
          service(muxreq).map(rep => Buf.ByteArray.Owned.extract(rep.body))
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
      ThriftMuxToMux andThen deserializingClassifier.newService(dest, label)
    }

    def newClient(dest: Name, label: String): ServiceFactory[ThriftClientRequest, Array[Byte]] = {
      clientId.foreach(id => ClientRegistry.export(params, "ClientId", id.name))
      ThriftMuxToMux andThen deserializingClassifier.newClient(dest, label)
    }

    /**
     * '''Experimental:''' This API is under construction.
     *
     * Create a [[thriftmux.MethodBuilder]] for a given destination.
     *
     * @see [[https://twitter.github.io/finagle/guide/MethodBuilder.html user guide]]
     */
    def methodBuilder(dest: String): thriftmux.MethodBuilder =
      thriftmux.MethodBuilder.from(dest, this)

    /**
     * '''Experimental:''' This API is under construction.
     *
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
    override def withRetryBackoff(backoff: Stream[Duration]): Client = super.withRetryBackoff(backoff)

    override def configured[P](psp: (P, Stack.Param[P])): Client = super.configured(psp)
  }

  val client: ThriftMux.Client = Client()
    .configured(Label("thrift"))
    .configured(Stats(ClientStatsReceiver))

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
   * Produce a [[com.twitter.finagle.ThriftMux.Client]] using the provided
   * client ID.
   */
  @deprecated("Use `ThriftMux.client.withClientId`", "6.22.0")
  def withClientId(clientId: ClientId): Client =
    client.withClientId(clientId)

  /**
   * Produce a [[com.twitter.finagle.ThriftMux.Client]] using the provided
   * protocolFactory.
   */
  @deprecated("Use `ThriftMux.client.withProtocolFactory`", "6.22.0")
  def withProtocolFactory(pf: TProtocolFactory): Client =
    client.withProtocolFactory(pf)

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
      params: Stack.Params = Mux.server.params + ProtocolLibrary("thriftmux"))
    extends StdStackServer[mux.Request, mux.Response, ServerMuxer] {

    protected type In = Buf
    protected type Out = Buf

    private[this] val statsReceiver = params[Stats].statsReceiver

    protected def copy1(
      stack: Stack[ServiceFactory[mux.Request, mux.Response]] = this.stack,
      params: Stack.Params = this.params
    ): ServerMuxer = copy(stack, params)

    protected def newListener(): Listener[In, Out] =
      params[Mux.param.MuxImpl].listener(params)

    protected def newDispatcher(
      transport: Transport[In, Out],
      service: Service[mux.Request, mux.Response]
    ): Closable = {
      val Lessor.Param(lessor) = params[Lessor.Param]
      val Mux.param.MaxFrameSize(frameSize) = params[Mux.param.MaxFrameSize]
      val muxStatsReceiver = statsReceiver.scope("mux")
      val param.ExceptionStatsHandler(excRecorder) = params[param.ExceptionStatsHandler]
      val param.Tracer(tracer) = params[param.Tracer]
      val Thrift.param.ProtocolFactory(pf) = params[Thrift.param.ProtocolFactory]

      val thriftEmulator = thriftmux.ThriftEmulator(
        transport, pf, statsReceiver.scope("thriftmux"))

      val negotiatedTrans = mux.Handshake.server(
        trans = thriftEmulator,
        version = Mux.LatestVersion,
        headers = Mux.Server.headers(_, frameSize),
        negotiate = Mux.negotiate(frameSize, muxStatsReceiver))

      val statsTrans = new StatsTransport(
        negotiatedTrans,
        excRecorder,
        muxStatsReceiver.scope("transport"))

      mux.ServerDispatcher.newRequestResponse(
        statsTrans,
        service,
        lessor,
        tracer,
        muxStatsReceiver)
    }
  }

  val serverMuxer = ServerMuxer()

  object Server {
    private val MuxToArrayFilter =
      new Filter[mux.Request, mux.Response, Array[Byte], Array[Byte]] {
        def apply(
          request: mux.Request, service: Service[Array[Byte], Array[Byte]]
        ): Future[mux.Response] = {
          val reqBytes = Buf.ByteArray.Owned.extract(request.body)
          service(reqBytes) map { repBytes =>
            mux.Response(Buf.ByteArray.Owned(repBytes))
          }
        }
      }

    // Convert unhandled exceptions to TApplicationExceptions, but pass
    // com.twitter.finagle.FailureFlags to mux for transmission.
    private[this] class ExnFilter(protocolFactory: TProtocolFactory)
      extends SimpleFilter[mux.Request, mux.Response]
    {
      def apply(
        request: mux.Request,
        service: Service[mux.Request, mux.Response]
      ): Future[mux.Response] =
        service(request).rescue {
          case f: FailureFlags[_] => Future.exception(f)
          case e if !e.isInstanceOf[TException] =>
            val msg = UncaughtAppExceptionFilter.writeExceptionMessage(
              request.body, e, protocolFactory)
            Future.value(mux.Response(msg))
        }
      }

    private[ThriftMux] val ExnHandler =
      new Stack.Module1[Thrift.param.ProtocolFactory, ServiceFactory[mux.Request, mux.Response]]
    {
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
  case class Server(
      muxer: StackServer[mux.Request, mux.Response] = serverMuxer)
    extends StackBasedServer[Array[Byte], Array[Byte]]
    with ThriftRichServer
    with Stack.Parameterized[Server]
    with CommonParams[Server]
    with WithServerTransport[Server]
    with WithServerSession[Server]
    with WithServerAdmissionControl[Server] {

    import Server.MuxToArrayFilter

    def stack: Stack[ServiceFactory[mux.Request, mux.Response]] =
      muxer.stack

    override protected val Label(serverLabel) = params[Label]

    override protected lazy val Stats(serverStats) = params[Stats]

    def params: Stack.Params = muxer.params

    protected val Thrift.param.ProtocolFactory(protocolFactory) =
      params[Thrift.param.ProtocolFactory]

    override val Thrift.Server.param.MaxReusableBufferSize(maxThriftBufferSize) =
      params[Thrift.Server.param.MaxReusableBufferSize]

    /**
     * Produce a `com.twitter.finagle.Thrift.Server` using the provided
     * `TProtocolFactory`.
     */
    def withProtocolFactory(pf: TProtocolFactory): Server =
      configured(Thrift.param.ProtocolFactory(pf))

    /**
     * Produce a [[com.twitter.finagle.Thrift.Server]] with the specified max
     * size of the reusable buffer for thrift responses. If this size
     * is exceeded, the buffer is not reused and a new buffer is
     * allocated for the next thrift response.
     * @param size Max size of the reusable buffer for thrift responses in bytes.
     */
    def withMaxReusableBufferSize(size: Int): Server =
      configured(Thrift.Server.param.MaxReusableBufferSize(size))

    def withParams(ps: Stack.Params): Server =
      copy(muxer = muxer.withParams(ps))

    private[this] val tracingFilter = new SimpleFilter[Array[Byte], Array[Byte]] {
      def apply(request: Array[Byte], svc: Service[Array[Byte], Array[Byte]]): Future[Array[Byte]] = {
        recordRpc(request)
        svc(request)
      }
    }

    def serve(
      addr: SocketAddress,
      factory: ServiceFactory[Array[Byte], Array[Byte]]
    ): ListeningServer = {
      muxer.serve(
        addr,
        MuxToArrayFilter.andThen(tracingFilter).andThen(factory))
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

  val server: Server = Server()
    .configured(Label("thrift"))
    .configured(Stats(ServerStatsReceiver))

  def serve(
    addr: SocketAddress,
    factory: ServiceFactory[Array[Byte], Array[Byte]]
  ): ListeningServer =
    server.serve(addr, factory)
}
