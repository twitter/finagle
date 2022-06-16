package com.twitter.finagle

import com.twitter.finagle.client.ClientRegistry
import com.twitter.finagle.client.StackClient
import com.twitter.finagle.client.StdStackClient
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.dispatch.ClientDispatcher
import com.twitter.finagle.naming.BindingFactory
import com.twitter.finagle.param.{
  ExceptionStatsHandler => _,
  Monitor => _,
  ResponseClassifier => _,
  Tracer => _,
  _
}
import com.twitter.finagle.server.Listener
import com.twitter.finagle.server.StackServer
import com.twitter.finagle.server.StdStackServer
import com.twitter.finagle.service.StatsFilter
import com.twitter.finagle.service.ResponseClassifier
import com.twitter.finagle.service.RetryBudget
import com.twitter.finagle.stats.ExceptionStatsHandler
import com.twitter.finagle.stats.SourceRole
import com.twitter.finagle.stats.StandardStatsReceiver
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.thrift.exp.partitioning.ThriftPartitioningService.ReqRepMarshallable
import com.twitter.finagle.thrift.exp.partitioning.PartitioningParams
import com.twitter.finagle.thrift.exp.partitioning.ThriftPartitioningService
import com.twitter.finagle.thrift.exp.partitioning.WithThriftPartitioningStrategy
import com.twitter.finagle.thrift.service.ThriftResponseClassifier
import com.twitter.finagle.thrift.ThriftUtil
import com.twitter.finagle.thrift.filter.ValidationReportingFilter
import com.twitter.finagle.thrift.transport.ThriftClientPreparer
import com.twitter.finagle.thrift.transport.netty4.Netty4Transport
import com.twitter.finagle.thrift.{ClientId => FinagleClientId, _}
import com.twitter.finagle.tracing.Tracer
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.transport.TransportContext
import com.twitter.scrooge.TReusableBuffer
import com.twitter.util.Closable
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.FuturePool
import com.twitter.util.Monitor
import java.net.SocketAddress
import java.util.concurrent.ExecutorService
import org.apache.thrift.protocol.TProtocolFactory

/**
 * Client and server for [[https://thrift.apache.org Apache Thrift]].
 * `Thrift` implements Thrift framed transport and binary protocol by
 * default, though custom protocol factories (i.e. wire encoding) may
 * be injected with `withProtocolFactory`. The client,
 * `Client[ThriftClientRequest, Array[Byte]]` provides direct access
 * to the thrift transport, but we recommend using code generation
 * through either [[https://github.com/twitter/scrooge Scrooge]] or
 * [[https://github.com/mariusaeriksen/thrift-finagle a fork]]
 * of the Apache generator. A rich API is provided to support
 * interfaces generated with either of these code generators.
 *
 * The client and server uses the standard thrift protocols, with
 * support for both framed and buffered transports. Finagle attempts
 * to upgrade the protocol in order to ship an extra envelope
 * carrying additional request metadata, containing, among other
 * things, request IDs for Finagle's RPC tracing facilities.
 *
 * The negotiation is simple: on connection establishment, an
 * improbably-named method is dispatched on the server. If that
 * method isn't found, we are dealing with a legacy thrift server,
 * and the standard protocol is used. If the remote server is also a
 * finagle server (or any other supporting this extension), we reply
 * to the request, and every subsequent request is dispatched with an
 * envelope carrying trace metadata. The envelope itself is also a
 * Thrift struct described
 * [[https://github.com/twitter/finagle/blob/release/finagle-thrift/src/main/thrift/tracing.thrift
 * here]].
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
 * `TestService.MethodPerEndpoint`. This is then passed
 * into `Thrift.Client.build`:
 *
 * {{{
 * Thrift.client.build[TestService.MethodPerEndpoint](
 *   addr, classOf[TestService.MethodPerEndpoint])
 * }}}
 *
 * However note that the Scala compiler can insert the latter
 * `Class` for us, for which another variant of `build` is
 * provided:
 *
 * {{{
 * Thrift.client.build[TestService.MethodPerEndpoint](addr)
 * }}}
 *
 * In Java, we need to provide the class object:
 *
 * {{{
 * TestService.MethodPerEndpoint client =
 *   Thrift.client.build(addr, TestService.MethodPerEndpoint.class);
 * }}}
 *
 * The client uses the standard thrift protocols, with support for
 * both framed and buffered transports. Finagle attempts to upgrade
 * the protocol in order to ship an extra envelope carrying trace IDs
 * and client IDs associated with the request. These are used by
 * Finagle's tracing facilities and may be collected via aggregators
 * like [[https://github.com/openzipkin/zipkin Zipkin]].
 *
 * The negotiation is simple: on connection establishment, an
 * improbably-named method is dispatched on the server. If that
 * method isn't found, we are dealing with a legacy thrift server,
 * and the standard protocol is used. If the remote server is also a
 * finagle server (or any other supporting this extension), we reply
 * to the request, and every subsequent request is dispatched with an
 * envelope carrying trace metadata. The envelope itself is also a
 * Thrift struct described [[https://github.com/twitter/finagle/blob/release/finagle-thrift/src/main/thrift/tracing.thrift here]].
 *
 * == Servers ==
 *
 * `TestService.MethodPerEndpoint` must be implemented and passed
 * into `serveIface`:
 *
 * {{{
 * // An echo service
 * ThriftMux.server.serveIface(":*", new TestService.MethodPerEndpoint {
 *   def query(x: String): Future[String] = Future.value(x)
 * })
 * }}}
 */
object Thrift
    extends Client[ThriftClientRequest, Array[Byte]]
    with Server[Array[Byte], Array[Byte]] {

  private val protocolLibraryName = "thrift"

  object param {

    /**
     * The vanilla Thrift `Transporter` and `Listener` factories deviate from other protocols in
     * the result of the netty pipeline: most other protocols expect to receive a framed `Buf`
     * while vanilla thrift produces an `Array[Byte]`. This has two related motivations. First, the
     * end result needed by the thrift implementations is an `Array[Byte]`, which is relatively
     * trivial to deal with and is a JVM native type so it's unnecessary to go through a `Buf`.
     * By avoiding an indirection through `Buf` we can avoid an unnecessary copy in the netty4
     * pipeline that would be required to ensure that the bytes were on the heap before
     * entering the Finagle transport types.
     */
    val protocolFactory: TProtocolFactory = Protocols.binaryFactory()

    val maxThriftBufferSize: Int = 16 * 1024

    case class ClientId(clientId: Option[FinagleClientId])
    implicit object ClientId extends Stack.Param[ClientId] {
      val default = ClientId(None)
    }

    case class ProtocolFactory(protocolFactory: TProtocolFactory)
    implicit object ProtocolFactory extends Stack.Param[ProtocolFactory] {
      val default = ProtocolFactory(protocolFactory)
    }

    /**
     * A `Param` to control whether a framed transport should be used.
     * If this is set to false, a buffered transport is used.  Framed
     * transports are enabled by default.
     *
     * @param enabled Whether a framed transport should be used.
     */
    case class Framed(enabled: Boolean)
    implicit object Framed extends Stack.Param[Framed] {
      val default = Framed(true)
    }

    /**
     * A `Param` to control upgrading the thrift protocol to TTwitter.
     *
     * @see The [[https://twitter.github.io/finagle/guide/Protocols.html?highlight=Twitter-upgraded#thrift user guide]] for details on Twitter-upgrade Thrift.
     */
    case class AttemptTTwitterUpgrade(upgrade: Boolean)
    implicit object AttemptTTwitterUpgrade extends Stack.Param[AttemptTTwitterUpgrade] {
      val default = AttemptTTwitterUpgrade(true)
    }

    /**
     * A `Param` to set the max size of a reusable buffer for the thrift response.
     * If the buffer size exceeds the specified value, the buffer is not reused,
     * and a new buffer is used for the next thrift response.
     * The default max size is 16Kb.
     *
     * @param maxReusableBufferSize Max buffer size in bytes.
     */
    case class MaxReusableBufferSize(maxReusableBufferSize: Int)
    implicit object MaxReusableBufferSize extends Stack.Param[MaxReusableBufferSize] {
      val default = MaxReusableBufferSize(maxThriftBufferSize)
    }

    /**
     * A `Param` that sets a factory to create a TReusableBuffer, the TReusableBuffer can be
     * shared with other client instances.
     * If this is set, MaxReusableBufferSize will be ignored.
     */
    case class TReusableBufferFactory(tReusableBufferFactory: () => TReusableBuffer)
    implicit object TReusableBufferFactory extends Stack.Param[TReusableBufferFactory] {
      val default = TReusableBufferFactory(RichClientParam.NO_THRIFT_REUSABLE_BUFFER_FACTORY)
    }

    /**
     * A `Param` to control whether to record per-endpoint stats.
     * If this is set to true, per-endpoint stats will be counted.
     *
     * @param enabled Whether to count per-endpoint stats
     */
    case class PerEndpointStats(enabled: Boolean)
    implicit object PerEndpointStats extends Stack.Param[PerEndpointStats] {
      val default = PerEndpointStats(false)
    }

    /**
     * Search for a Scrooge-generated service stub class from the service
     * class in clients and servers
     */
    private object ServiceClassUtil {

      /**
       * Check that the baseName is a valid Thrift IDL service class.
       * Return baseName if we find a server/client class with the ifaceSuffix
       * in the Scrooge-generated Scala or Java code
       */
      private def checkClass[Iface](
        baseName: String,
        ifaceSuffix: String
      ): Option[String] = {
        ThriftUtil.findClass[Iface](baseName + ifaceSuffix).map(_ => baseName)
      }

      /**
       * Search for the Thrift IDL service class. Strip any Scrooge-generated
       * suffixes from the class. Check that the class has a FinagleService,
       * FinagledClient, Service, or ServiceToClient member class.
       */
      private def searchBySuffix[Iface](cls: Class[_], ifaceSuffix: String): Option[String] = {
        val baseName: String = ThriftUtil.stripSuffix(cls)
        checkClass[Iface](baseName, ifaceSuffix)
      }

      private def searchServer(cls: Class[_]): Option[String] = {
        searchBySuffix[ThriftUtil.BinaryService](cls, ThriftUtil.FinagledServerSuffixJava)
          .orElse(
            searchBySuffix[ThriftUtil.BinaryService](cls, ThriftUtil.FinagledServerSuffixScala))
          .orElse {
            (Option(cls.getSuperclass) ++ cls.getInterfaces).view.flatMap(searchServer).headOption
          }
      }

      private def searchClient(cls: Class[_]): Option[String] = {
        searchBySuffix(cls, ThriftUtil.FinagledClientSuffixJava)
          .orElse(searchBySuffix(cls, ThriftUtil.FinagledClientSuffixScala))
          .orElse {
            (Option(cls.getSuperclass) ++ cls.getInterfaces).view.flatMap(searchClient).headOption
          }
      }

      /**
       * Runs a recursive search for a Scrooge-generated service stub class, starting with the class
       * passed to a Finagle client or a server. This function isn't tail-recursive so there is a
       * risk it can run out of stack space. We, however, don't anticipate this to happen since
       * service stubs hierarchies are finite and frankly very short (0 to 2 hops at most).
       */
      def search(impl: Class[_]): Option[String] = {
        searchServer(impl)
          .orElse(searchClient(impl))
      }
    }

    /**
     * A `Param` that captures a class of a Scrooge-generated service stub that's associated
     * with a given client or server.
     */
    final case class ServiceClass(clazz: Option[Class[_]]) {

      /**
       * Extracts the Thrift IDL FQN from the Scrooge-generated service class in `clazz`. The main
       * purpose of this method is to drop the Scrooge-specific suffix from the class name, leaving
       * only domain-relevant name behind.
       */
      val fullyQualifiedName: Option[String] = clazz.flatMap(ServiceClassUtil.search)
    }

    implicit object ServiceClass extends Stack.Param[ServiceClass] {
      val default = ServiceClass(None)
    }
  }

  object Client extends ThriftClient {
    private val preparer: Stackable[ServiceFactory[ThriftClientRequest, Array[Byte]]] =
      new Stack.ModuleParams[ServiceFactory[ThriftClientRequest, Array[Byte]]] {
        override def parameters: Seq[Stack.Param[_]] = Nil
        override val role: Stack.Role = StackClient.Role.prepConn
        override val description = "Prepare TTwitter thrift connection"
        def make(
          params: Stack.Params,
          next: ServiceFactory[ThriftClientRequest, Array[Byte]]
        ): ServiceFactory[ThriftClientRequest, Array[Byte]] = {
          val Label(label) = params[Label]
          val param.ClientId(clientId) = params[param.ClientId]
          val Thrift.param.ProtocolFactory(pf) = params[Thrift.param.ProtocolFactory]
          val preparer = ThriftClientPreparer(pf, label, clientId)
          preparer.prepare(next, params)
        }
      }

    // We must do 'preparation' this way in order to let Finagle set up tracing & so on.
    private val stack: Stack[ServiceFactory[ThriftClientRequest, Array[Byte]]] = {

      /** Thrift helper for message marshalling */
      object ThriftMarshallable extends ReqRepMarshallable[ThriftClientRequest, Array[Byte]] {
        def framePartitionedRequest(
          rawRequest: ThriftClientRequest,
          original: ThriftClientRequest
        ): ThriftClientRequest = rawRequest
        def isOneway(original: ThriftClientRequest): Boolean = original.oneway
        def fromResponseToBytes(rep: Array[Byte]): Array[Byte] = rep
        val emptyResponse: Array[Byte] = Array.emptyByteArray
      }

      // we insert  validationReporting filter to thrift client after the statsFilter
      // because we will be able to see all metrics the statsFilter sees
      StackClient.newStack
        .replace(StackClient.Role.prepConn, preparer)
        .insertAfter(BindingFactory.role, ThriftPartitioningService.module(ThriftMarshallable))
        .insertAfter(
          StatsFilter.role,
          ValidationReportingFilter.module[ThriftClientRequest, Array[Byte]])
    }

    private def params: Stack.Params = StackClient.defaultParams +
      ProtocolLibrary(protocolLibraryName)
  }

  /**
   * A ThriftMux `com.twitter.finagle.Client`.
   *
   * @see [[https://twitter.github.io/finagle/guide/Configuration.html#clients-and-servers Configuration]] documentation
   * @see [[https://twitter.github.io/finagle/guide/Protocols.html#thrift Thrift]] documentation
   * @see [[https://twitter.github.io/finagle/guide/Protocols.html#mux Mux]] documentation
   */
  case class Client(
    stack: Stack[ServiceFactory[ThriftClientRequest, Array[Byte]]] = Client.stack,
    params: Stack.Params = Client.params)
      extends StdStackClient[ThriftClientRequest, Array[Byte], Client]
      with WithSessionPool[Client]
      with WithDefaultLoadBalancer[Client]
      with WithThriftPartitioningStrategy[Client]
      with ThriftRichClient {

    protected def copy1(
      stack: Stack[ServiceFactory[ThriftClientRequest, Array[Byte]]] = this.stack,
      params: Stack.Params = this.params
    ): Client = copy(stack, params)

    protected val clientParam: RichClientParam = RichClientParam(
      protocolFactory = params[param.ProtocolFactory].protocolFactory,
      maxThriftBufferSize = params[Thrift.param.MaxReusableBufferSize].maxReusableBufferSize,
      thriftReusableBufferFactory =
        params[Thrift.param.TReusableBufferFactory].tReusableBufferFactory,
      clientStats = params[Stats].statsReceiver,
      responseClassifier = params[com.twitter.finagle.param.ResponseClassifier].responseClassifier,
      perEndpointStats = params[Thrift.param.PerEndpointStats].enabled
    )

    protected lazy val Label(defaultClientName) = params[Label]

    protected type In = ThriftClientRequest
    protected type Out = Array[Byte]
    protected type Context = TransportContext

    protected def newTransporter(addr: SocketAddress): Transporter[In, Out, Context] =
      Netty4Transport.Client(params)(addr)

    protected def newDispatcher(
      transport: Transport[ThriftClientRequest, Array[Byte]] { type Context <: Client.this.Context }
    ): Service[ThriftClientRequest, Array[Byte]] =
      new ThriftSerialClientDispatcher(
        transport,
        params[Stats].statsReceiver.scope(ClientDispatcher.StatsScope)
      )

    def withProtocolFactory(protocolFactory: TProtocolFactory): Client =
      configured(param.ProtocolFactory(protocolFactory))

    def withClientId(clientId: FinagleClientId): Client =
      configured(Thrift.param.ClientId(Some(clientId)))

    /**
     * Use a buffered transport instead of the default framed transport.
     * In almost all cases, the default framed transport should be used.
     */
    def withBufferedTransport: Client =
      configured(Thrift.param.Framed(false))

    def withAttemptTTwitterUpgrade: Client =
      configured(param.AttemptTTwitterUpgrade(true))

    def withNoAttemptTTwitterUpgrade: Client =
      configured(param.AttemptTTwitterUpgrade(false))

    /**
     * Produce a [[com.twitter.finagle.Thrift.Client]] with the specified max
     * size of the reusable buffer for thrift responses. If this size
     * is exceeded, the buffer is not reused and a new buffer is
     * allocated for the next thrift response.
     * The default max size is 16Kb.
     *
     * @note MaxReusableBufferSize will be ignored if TReusableBufferFactory is set.
     *
     * @param size Max size of the reusable buffer for thrift responses in bytes.
     */
    def withMaxReusableBufferSize(size: Int): Client =
      configured(param.MaxReusableBufferSize(size))

    /**
     * Produce a [[com.twitter.finagle.Thrift.Client]] with a factory creates new
     * TReusableBuffer, the TReusableBuffer can be shared with other client instance.
     * If set, the MaxReusableBufferSize will be ignored.
     */
    def withTReusableBufferFactory(tReusableBufferFactory: () => TReusableBuffer): Client =
      configured(param.TReusableBufferFactory(tReusableBufferFactory))

    /**
     * Produce a [[com.twitter.finagle.Thrift.Client]] with per-endpoint stats filters
     */
    def withPerEndpointStats: Client =
      configured(param.PerEndpointStats(true))

    def clientId: Option[FinagleClientId] = params[Thrift.param.ClientId].clientId

    private[this] def withDeserializingClassifier: Client = {
      // Note: what type of deserializer used is important if none is specified
      // so that we keep the prior behavior of Thrift exceptions
      // being counted as a success. Otherwise, even using the default
      // ResponseClassifier would then see that response as a `Throw` and thus
      // a failure. So, when none is specified, a "deserializing-only"
      // classifier is used to make when deserialization happens in the stack
      // uniform whether or not a `ResponseClassifier` is wired up.
      val classifier = if (params.contains[com.twitter.finagle.param.ResponseClassifier]) {
        ThriftResponseClassifier.usingDeserializeCtx(
          params[com.twitter.finagle.param.ResponseClassifier].responseClassifier
        )
      } else {
        ThriftResponseClassifier.DeserializeCtxOnly
      }
      configured(com.twitter.finagle.param.ResponseClassifier(classifier))
    }

    private def superNewClient(dest: Name, label: String) =
      super.newClient(dest, label)

    override def newClient(
      dest: Name,
      label: String
    ): ServiceFactory[ThriftClientRequest, Array[Byte]] = {
      clientId.foreach(id => ClientRegistry.export(params, "ClientId", id.name))
      withDeserializingClassifier.superNewClient(dest, label)
    }

    // Java-friendly forwarders
    // See https://issues.scala-lang.org/browse/SI-8905
    override val withSessionPool: SessionPoolingParams[Client] =
      new SessionPoolingParams(this)
    override val withLoadBalancer: DefaultLoadBalancingParams[Client] =
      new DefaultLoadBalancingParams(this)
    override val withTransport: ClientTransportParams[Client] =
      new ClientTransportParams(this)
    override val withSession: ClientSessionParams[Client] =
      new ClientSessionParams(this)
    override val withSessionQualifier: SessionQualificationParams[Client] =
      new SessionQualificationParams(this)
    override val withAdmissionControl: ClientAdmissionControlParams[Client] =
      new ClientAdmissionControlParams(this)
    override val withPartitioning: PartitioningParams[Client] =
      new PartitioningParams(this)

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
    override def withRetryBackoff(backoff: Backoff): Client =
      super.withRetryBackoff(backoff)

    override def withStack(stack: Stack[ServiceFactory[ThriftClientRequest, Array[Byte]]]): Client =
      super.withStack(stack)
    override def withStack(
      fn: Stack[ServiceFactory[ThriftClientRequest, Array[Byte]]] => Stack[
        ServiceFactory[ThriftClientRequest, Array[Byte]]
      ]
    ): Client =
      super.withStack(fn)
    override def withExecutionOffloaded(executor: ExecutorService): Client =
      super.withExecutionOffloaded(executor)
    override def withExecutionOffloaded(pool: FuturePool): Client =
      super.withExecutionOffloaded(pool)
    override def configured[P](psp: (P, Stack.Param[P])): Client = super.configured(psp)
    override def filtered(
      filter: Filter[ThriftClientRequest, Array[Byte], ThriftClientRequest, Array[Byte]]
    ): Client =
      super.filtered(filter)
  }

  def client: Thrift.Client = Client()

  def newService(dest: Name, label: String): Service[ThriftClientRequest, Array[Byte]] =
    client.newService(dest, label)

  def newClient(dest: Name, label: String): ServiceFactory[ThriftClientRequest, Array[Byte]] =
    client.newClient(dest, label)

  object Server {
    private val preparer =
      new Stack.ModuleParams[ServiceFactory[Array[Byte], Array[Byte]]] {
        override def parameters: Seq[Stack.Param[_]] = Nil
        override val role: Stack.Role = StackClient.Role.prepConn
        override val description = "Prepare TTwitter thrift connection"
        def make(
          params: Stack.Params,
          next: ServiceFactory[Array[Byte], Array[Byte]]
        ): ServiceFactory[Array[Byte], Array[Byte]] = {
          val Label(label) = params[Label]
          val Thrift.param.ProtocolFactory(pf) = params[Thrift.param.ProtocolFactory]
          val preparer = ThriftServerPreparer(pf, label)
          preparer.prepare(next, params)
        }
      }

    // Set an empty ServerToReqRep context in the stack. Scrooge generated finagle service should
    // then set the value.
    private val ServerToReqRepPreparer =
      new Stack.Module0[ServiceFactory[Array[Byte], Array[Byte]]] {
        val role: Stack.Role = Stack.Role("ServerToReqRep Preparer")
        val description: String = "Set an empty bytes to ReqRep context in the local contexts, " +
          "scrooge generated service should set the value."
        def make(
          next: ServiceFactory[Array[Byte], Array[Byte]]
        ): ServiceFactory[Array[Byte], Array[Byte]] = {
          val svcDeserializeCtxFilter = new SimpleFilter[Array[Byte], Array[Byte]] {
            def apply(
              request: Array[Byte],
              service: Service[Array[Byte], Array[Byte]]
            ): Future[Array[Byte]] = {
              val deserCtx = new ServerToReqRep
              Contexts.local.let(ServerToReqRep.Key, deserCtx) {
                service(request)
              }
            }
          }
          svcDeserializeCtxFilter.andThen(next)
        }
      }

    // we insert  validationReporting filter to thrift server after the statsFilter
    // because we will be able to see all metrics the statsFilter sees
    private val stack: Stack[ServiceFactory[Array[Byte], Array[Byte]]] = StackServer.newStack
      .insertBefore(StackServer.Role.preparer, ServerToReqRepPreparer)
      .insertAfter(StatsFilter.role, ValidationReportingFilter.module[Array[Byte], Array[Byte]])
      .replace(StackServer.Role.preparer, preparer)

    private def params: Stack.Params = StackServer.defaultParams +
      ProtocolLibrary(protocolLibraryName) +
      StandardStats(
        stats.StatsAndClassifier(
          new StandardStatsReceiver(SourceRole.Server, protocolLibraryName),
          ThriftResponseClassifier.ThriftExceptionsAsFailures
        ))
  }

  /**
   * A ThriftMux `com.twitter.finagle.Server`.
   *
   * @see [[https://twitter.github.io/finagle/guide/Configuration.html#clients-and-servers Configuration]] documentation
   * @see [[https://twitter.github.io/finagle/guide/Protocols.html#thrift Thrift]] documentation
   */
  case class Server(
    stack: Stack[ServiceFactory[Array[Byte], Array[Byte]]] = Server.stack,
    params: Stack.Params = Server.params)
      extends StdStackServer[Array[Byte], Array[Byte], Server]
      with ThriftRichServer {
    protected def copy1(
      stack: Stack[ServiceFactory[Array[Byte], Array[Byte]]] = this.stack,
      params: Stack.Params = this.params
    ): Server = copy(stack, params)

    protected type In = Array[Byte]
    protected type Out = Array[Byte]
    protected type Context = TransportContext

    protected val serverParam: RichServerParam = RichServerParam(
      protocolFactory = params[Thrift.param.ProtocolFactory].protocolFactory,
      maxThriftBufferSize = params[Thrift.param.MaxReusableBufferSize].maxReusableBufferSize,
      serverStats = params[Stats].statsReceiver,
      responseClassifier = params[com.twitter.finagle.param.ResponseClassifier].responseClassifier,
      perEndpointStats = params[Thrift.param.PerEndpointStats].enabled
    )

    private[this] def withDeserializingClassifier: Server = {
      // Note: what type of deserializer used is important if none is specified
      // so that we keep the prior behavior of Thrift exceptions
      // being counted as a success. Otherwise, even using the default
      // ResponseClassifier would then see that response as a `Throw` and thus
      // a failure. So, when none is specified, a "deserializing-only"
      // classifier is used to make when deserialization happens in the stack
      // uniform whether or not a `ResponseClassifier` is wired up.
      val classifier = if (params.contains[com.twitter.finagle.param.ResponseClassifier]) {
        ThriftResponseClassifier.usingReqRepCtx(
          params[com.twitter.finagle.param.ResponseClassifier].responseClassifier
        )
      } else {
        ThriftResponseClassifier.ReqRepCtxOnly
      }
      configured(com.twitter.finagle.param.ResponseClassifier(classifier))
    }

    private def superServe(
      addr: SocketAddress,
      service: ServiceFactory[Array[Byte], Array[Byte]]
    ): ListeningServer = super.serve(addr, service)

    override def serve(
      addr: SocketAddress,
      service: ServiceFactory[Array[Byte], Array[Byte]]
    ): ListeningServer = {
      withDeserializingClassifier.superServe(addr, service)
    }

    protected def newListener(): Listener[In, Out, Context] = Netty4Transport.Server(params)

    protected def newDispatcher(
      transport: Transport[In, Out] { type Context <: Server.this.Context },
      service: Service[Array[Byte], Array[Byte]]
    ): Closable = new ThriftSerialServerDispatcher(transport, service)

    /**
     * Configure the service class that may be used with this server to
     * collect instrumentation metadata. This is not necessary to run a
     * service.
     *
     * @note that when using the `.serveIface` methods this is unnecessary.
     */
    def withServiceClass(clazz: Class[_]): Server =
      configured(param.ServiceClass(Some(clazz)))

    def withProtocolFactory(protocolFactory: TProtocolFactory): Server =
      configured(param.ProtocolFactory(protocolFactory))

    def withBufferedTransport(): Server =
      configured(param.Framed(false))

    /**
     * Produce a [[com.twitter.finagle.Thrift.Server]] with the specified max
     * size of the reusable buffer for thrift responses. If this size
     * is exceeded, the buffer is not reused and a new buffer is
     * allocated for the next thrift response.
     * The default max size is 16Kb.
     *
     * @param size Max size of the reusable buffer for thrift responses in bytes.
     */
    def withMaxReusableBufferSize(size: Int): Server =
      configured(param.MaxReusableBufferSize(size))

    /**
     * Produce a [[com.twitter.finagle.Thrift.Server]] with per-endpoint stats filters
     */
    def withPerEndpointStats: Server =
      configured(param.PerEndpointStats(true))

    // Java-friendly forwarders
    // See https://issues.scala-lang.org/browse/SI-8905
    override val withAdmissionControl: ServerAdmissionControlParams[Server] =
      new ServerAdmissionControlParams(this)
    override val withSession: ServerSessionParams[Server] =
      new ServerSessionParams(this)
    override val withTransport: ServerTransportParams[Server] =
      new ServerTransportParams(this)

    override def withLabel(label: String): Server = super.withLabel(label)
    override def withStatsReceiver(statsReceiver: StatsReceiver): Server =
      super.withStatsReceiver(statsReceiver)
    override def withMonitor(monitor: Monitor): Server = super.withMonitor(monitor)
    override def withTracer(tracer: Tracer): Server = super.withTracer(tracer)
    override def withExceptionStatsHandler(exceptionStatsHandler: ExceptionStatsHandler): Server =
      super.withExceptionStatsHandler(exceptionStatsHandler)
    override def withRequestTimeout(timeout: Duration): Server = super.withRequestTimeout(timeout)

    override def withStack(stack: Stack[ServiceFactory[Array[Byte], Array[Byte]]]): Server =
      super.withStack(stack)

    override def withStack(
      fn: Stack[ServiceFactory[Array[Byte], Array[Byte]]] => Stack[
        ServiceFactory[Array[Byte], Array[Byte]]
      ]
    ): Server =
      super.withStack(fn)
    override def withExecutionOffloaded(executor: ExecutorService): Server =
      super.withExecutionOffloaded(executor)
    override def withExecutionOffloaded(pool: FuturePool): Server =
      super.withExecutionOffloaded(pool)
    override def configured[P](psp: (P, Stack.Param[P])): Server = super.configured(psp)
  }

  def server: Thrift.Server = Server()

  def serve(
    addr: SocketAddress,
    service: ServiceFactory[Array[Byte], Array[Byte]]
  ): ListeningServer = server.serve(addr, service)
}
