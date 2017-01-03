package com.twitter.finagle

import com.twitter.finagle.client.{StackClient, StdStackClient, Transporter}
import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.param.{ExceptionStatsHandler => _, Monitor => _, ResponseClassifier => _, Tracer => _, _}
import com.twitter.finagle.server.{Listener, ServerInfo, StackServer, StdStackServer}
import com.twitter.finagle.service.{ResponseClassifier, RetryBudget}
import com.twitter.finagle.stats.{ExceptionStatsHandler, StatsReceiver}
import com.twitter.finagle.thrift.{ClientId => _, _}
import com.twitter.finagle.thrift.service.ThriftResponseClassifier
import com.twitter.finagle.thrift.transport.ThriftClientPreparer
import com.twitter.finagle.thrift.transport.netty3.Netty3Transport
import com.twitter.finagle.thrift.transport.netty4.Netty4Transport
import com.twitter.finagle.toggle.Toggle
import com.twitter.finagle.tracing.Tracer
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Closable, Duration, Monitor}
import java.net.SocketAddress
import org.apache.thrift.protocol.TProtocolFactory

/**
 * Client and server for [[http://thrift.apache.org Apache Thrift]].
 * `Thrift` implements Thrift framed transport and binary protocol by
 * default, though custom protocol factories (i.e. wire encoding) may
 * be injected with `withProtocolFactory`. The client,
 * `Client[ThriftClientRequest, Array[Byte]]` provides direct access
 * to the thrift transport, but we recommend using code generation
 * through either [[https://github.com/twitter/scrooge Scrooge]] or
 * [[https://github.com/mariusaeriksen/thrift-0.5.0-finagle a fork]]
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
 * [[https://github.com/twitter/finagle/blob/master/finagle-thrift/src/main/thrift/tracing.thrift
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
 * `TestService.FutureIface`. This is then passed
 * into `Thrift.Client.newIface`:
 *
 * {{{
 * Thrift.client.newIface[TestService.FutureIface](
 *   addr, classOf[TestService.FutureIface])
 * }}}
 *
 * However note that the Scala compiler can insert the latter
 * `Class` for us, for which another variant of `newIface` is
 * provided:
 *
 * {{{
 * Thrift.client.newIface[TestService.FutureIface](addr)
 * }}}
 *
 * In Java, we need to provide the class object:
 *
 * {{{
 * TestService.FutureIface client =
 *   Thrift.client.newIface(addr, TestService.FutureIface.class);
 * }}}
 *
 * The client uses the standard thrift protocols, with support for
 * both framed and buffered transports. Finagle attempts to upgrade
 * the protocol in order to ship an extra envelope carrying trace IDs
 * and client IDs associated with the request. These are used by
 * Finagle's tracing facilities and may be collected via aggregators
 * like [[http://twitter.github.com/zipkin/ Zipkin]].
 *
 * The negotiation is simple: on connection establishment, an
 * improbably-named method is dispatched on the server. If that
 * method isn't found, we are dealing with a legacy thrift server,
 * and the standard protocol is used. If the remote server is also a
 * finagle server (or any other supporting this extension), we reply
 * to the request, and every subsequent request is dispatched with an
 * envelope carrying trace metadata. The envelope itself is also a
 * Thrift struct described [[https://github.com/twitter/finagle/blob/master/finagle-thrift/src/main/thrift/tracing.thrift here]].
 *
 * == Servers ==
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
 */
object Thrift
  extends Client[ThriftClientRequest, Array[Byte]]
  with Server[Array[Byte], Array[Byte]] {

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
  case class ThriftImpl(
      transporter: Stack.Params => Transporter[ThriftClientRequest, Array[Byte]],
      listener: Stack.Params => Listener[Array[Byte], Array[Byte]]) {

    def mk(): (ThriftImpl, Stack.Param[ThriftImpl]) = (this, ThriftImpl.param)

  }

  object ThriftImpl {
    private[this] val UseNetty4ToggleId: String = "com.twitter.finagle.thrift.UseNetty4"
    private[this] val netty4Toggle: Toggle[Int] = Toggles(UseNetty4ToggleId)
    private[this] def useNetty4: Boolean = netty4Toggle(ServerInfo().id.hashCode)

    val Netty3: ThriftImpl = ThriftImpl(Netty3Transport.Client, Netty3Transport.Server)
    val Netty4: ThriftImpl = ThriftImpl(Netty4Transport.Client, Netty4Transport.Server)

    implicit val param: Stack.Param[ThriftImpl] = Stack.Param(
      if (useNetty4) Netty4
      else Netty3
    )
  }

  val protocolFactory: TProtocolFactory = Protocols.binaryFactory()

  // Planned deprecation. Use `Thrift.Server.maxThriftBufferSize` instead.
  val maxThriftBufferSize: Int = 16 * 1024

  object param {
    case class ClientId(clientId: Option[thrift.ClientId])
    implicit object ClientId extends Stack.Param[ClientId] {
      val default = ClientId(None)
    }

    case class ProtocolFactory(protocolFactory: TProtocolFactory)
    implicit object ProtocolFactory extends Stack.Param[ProtocolFactory] {
      val default = ProtocolFactory(Protocols.binaryFactory())
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
  }

  object Client {
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
    val stack: Stack[ServiceFactory[ThriftClientRequest, Array[Byte]]] = StackClient.newStack
      .replace(StackClient.Role.prepConn, preparer)
  }

  /**
   * A ThriftMux `com.twitter.finagle.Client`.
   *
   * @see [[http://twitter.github.io/finagle/guide/Configuration.html#clients-and-servers Configuration]] documentation
   * @see [[http://twitter.github.io/finagle/guide/Protocols.html#thrift Thrift]] documentation
   * @see [[http://twitter.github.io/finagle/guide/Protocols.html#mux Mux]] documentation
   */
  case class Client(
      stack: Stack[ServiceFactory[ThriftClientRequest, Array[Byte]]] = Client.stack,
      params: Stack.Params = StackClient.defaultParams + ProtocolLibrary("thrift"))
    extends StdStackClient[ThriftClientRequest, Array[Byte], Client]
    with WithSessionPool[Client]
    with WithDefaultLoadBalancer[Client]
    with ThriftRichClient {

    protected def copy1(
      stack: Stack[ServiceFactory[ThriftClientRequest, Array[Byte]]] = this.stack,
      params: Stack.Params = this.params
    ): Client = copy(stack, params)

    protected lazy val Label(defaultClientName) = params[Label]

    protected type In = ThriftClientRequest
    protected type Out = Array[Byte]

    protected val param.ProtocolFactory(protocolFactory) = params[param.ProtocolFactory]
    override protected lazy val Stats(stats) = params[Stats]

    protected def newTransporter(): Transporter[In, Out] = params[ThriftImpl].transporter(params)

    protected def newDispatcher(
      transport: Transport[ThriftClientRequest, Array[Byte]]
    ): Service[ThriftClientRequest, Array[Byte]] =
      new ThriftSerialClientDispatcher(
        transport,
        params[Stats].statsReceiver.scope(GenSerialClientDispatcher.StatsScope)
      )

    def withProtocolFactory(protocolFactory: TProtocolFactory): Client =
      configured(param.ProtocolFactory(protocolFactory))

    def withClientId(clientId: thrift.ClientId): Client =
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

    def clientId: Option[thrift.ClientId] = params[Thrift.param.ClientId].clientId

    private[this] def deserializingClassifier: Client = {
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
    ): ServiceFactory[ThriftClientRequest, Array[Byte]] =
      deserializingClassifier.superNewClient(dest, label)

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

    override def withLabel(label: String): Client = super.withLabel(label)
    override def withStatsReceiver(statsReceiver: StatsReceiver): Client = super.withStatsReceiver(statsReceiver)
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
    override def filtered(filter: Filter[ThriftClientRequest, Array[Byte], ThriftClientRequest, Array[Byte]]): Client =
      super.filtered(filter)
  }

  val client: Thrift.Client = Client()

  def newService(
    dest: Name,
    label: String
  ): Service[ThriftClientRequest, Array[Byte]] =
    client.newService(dest, label)

  def newClient(
    dest: Name,
    label: String
  ): ServiceFactory[ThriftClientRequest, Array[Byte]] =
    client.newClient(dest, label)

  @deprecated("Use `Thrift.client.withProtocolFactory`", "6.22.0")
  def withProtocolFactory(protocolFactory: TProtocolFactory): Client =
    client.withProtocolFactory(protocolFactory)

  @deprecated("Use `Thrift.client.withClientId`", "6.22.0")
  def withClientId(clientId: thrift.ClientId): Client =
    client.withClientId(clientId)

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

    val maxThriftBufferSize: Int = 16 * 1024

    object param {
      /**
       * A `Param` to set the max size of a reusable buffer for the thrift response.
       * If the buffer size exceeds the specified value, the buffer is not reused,
       * and a new buffer is used for the next thrift response.
       *
       * @param maxReusableBufferSize Max buffer size in bytes.
       */
      case class MaxReusableBufferSize(maxReusableBufferSize: Int)
      implicit object MaxReusableBufferSize extends Stack.Param[MaxReusableBufferSize] {
        val default = MaxReusableBufferSize(maxThriftBufferSize)
      }
    }

    val stack: Stack[ServiceFactory[Array[Byte], Array[Byte]]] = StackServer.newStack
      .replace(StackServer.Role.preparer, preparer)
  }

  /**
   * A ThriftMux `com.twitter.finagle.Server`.
   *
   * @see [[http://twitter.github.io/finagle/guide/Configuration.html#clients-and-servers Configuration]] documentation
   * @see [[http://twitter.github.io/finagle/guide/Protocols.html#thrift Thrift]] documentation
   */
  case class Server(
    stack: Stack[ServiceFactory[Array[Byte], Array[Byte]]] = Server.stack,
    params: Stack.Params = StackServer.defaultParams + ProtocolLibrary("thrift")
  ) extends StdStackServer[Array[Byte], Array[Byte], Server] with ThriftRichServer {
    protected def copy1(
      stack: Stack[ServiceFactory[Array[Byte], Array[Byte]]] = this.stack,
      params: Stack.Params = this.params
    ): Server = copy(stack, params)

    protected type In = Array[Byte]
    protected type Out = Array[Byte]

    protected val param.ProtocolFactory(protocolFactory) = params[param.ProtocolFactory]

    override val Server.param.MaxReusableBufferSize(maxThriftBufferSize) =
      params[Server.param.MaxReusableBufferSize]

    protected def newListener(): Listener[In, Out] = params[ThriftImpl].listener(params)

    protected def newDispatcher(
      transport: Transport[In, Out],
      service: Service[Array[Byte], Array[Byte]]
    ): Closable = new ThriftSerialServerDispatcher(transport, service)

    def withProtocolFactory(protocolFactory: TProtocolFactory): Server =
      configured(param.ProtocolFactory(protocolFactory))

    def withBufferedTransport(): Server =
      configured(param.Framed(false))

    /**
     * Produce a [[com.twitter.finagle.Thrift.Server]] with the specified max
     * size of the reusable buffer for thrift responses. If this size
     * is exceeded, the buffer is not reused and a new buffer is
     * allocated for the next thrift response.
     * @param size Max size of the reusable buffer for thrift responses in bytes.
     */
    def withMaxReusableBufferSize(size: Int): Server =
      configured(Server.param.MaxReusableBufferSize(size))

    // Java-friendly forwarders
    // See https://issues.scala-lang.org/browse/SI-8905
    override val withAdmissionControl: ServerAdmissionControlParams[Server] =
      new ServerAdmissionControlParams(this)
    override val withSession: SessionParams[Server] =
      new SessionParams(this)
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

    override def configured[P](psp: (P, Stack.Param[P])): Server = super.configured(psp)
  }

  val server: Thrift.Server = Server()

  def serve(
    addr: SocketAddress,
    service: ServiceFactory[Array[Byte], Array[Byte]]
  ): ListeningServer = server.serve(addr, service)
}

