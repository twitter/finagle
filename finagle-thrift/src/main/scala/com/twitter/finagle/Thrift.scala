package com.twitter.finagle

import com.twitter.finagle.client.{StdStackClient, StackClient, Transporter}
import com.twitter.finagle.dispatch.{GenSerialClientDispatcher, SerialClientDispatcher, SerialServerDispatcher}
import com.twitter.finagle.netty3.{Netty3Transporter, Netty3Listener}
import com.twitter.finagle.param.{Monitor => _, ResponseClassifier => _, ExceptionStatsHandler => _, Tracer => _, _}
import com.twitter.finagle.server.{StdStackServer, StackServer, Listener}
import com.twitter.finagle.service.{ResponseClassifier, RetryBudget}
import com.twitter.finagle.stats.{ExceptionStatsHandler, StatsReceiver}
import com.twitter.finagle.thrift.service.ThriftResponseClassifier
import com.twitter.finagle.thrift.{ClientId => _, _}
import com.twitter.finagle.tracing.Tracer
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Duration, Monitor}
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
 * $clientExample
 *
 * $thriftUpgrade
 *
 * == Servers ==
 *
 * $serverExample
 *
 * @define clientExampleObject Thrift
 * @define serverExampleObject Thrift
 */
object Thrift extends Client[ThriftClientRequest, Array[Byte]] with ThriftRichClient
    with Server[Array[Byte], Array[Byte]] with ThriftRichServer {

  val protocolFactory: TProtocolFactory = Protocols.binaryFactory()

  protected lazy val Label(defaultClientName) = client.params[Label]

  protected def params: Stack.Params = client.params

  override protected lazy val Stats(stats) = client.params[Stats]

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
     * @param enabled Whether a framed transport should be used.
     */
    case class Framed(enabled: Boolean)
    implicit object Framed extends Stack.Param[Framed] {
      val default = Framed(true)
    }

    /**
     * A `Param` to set the max size of a reusable buffer for the thrift response.
     * If the buffer size exceeds the specified value, the buffer is not reused,
     * and a new buffer is used for the next thrift response.
     * @param maxReusableBufferSize Max buffer size in bytes.
     */
    case class MaxReusableBufferSize(maxReusableBufferSize: Int)
    implicit object MaxReusableBufferSize extends Stack.Param[MaxReusableBufferSize] {
      val default = MaxReusableBufferSize(maxThriftBufferSize)
    }

    /**
     * A `Param` to control upgrading the thrift protocol to TTwitter.
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
        override val role = StackClient.Role.prepConn
        override val description = "Prepare TTwitter thrift connection"
        def make(
          params: Stack.Params,
          next: ServiceFactory[ThriftClientRequest, Array[Byte]]
        ) = {
          val Label(label) = params[Label]
          val param.ClientId(clientId) = params[param.ClientId]
          val param.ProtocolFactory(pf) = params[param.ProtocolFactory]
          val preparer = new ThriftClientPreparer(pf, label, clientId)
          preparer.prepare(next, params)
        }
      }

    // We must do 'preparation' this way in order to let Finagle set up tracing & so on.
    val stack: Stack[ServiceFactory[ThriftClientRequest, Array[Byte]]] = StackClient.newStack
      .replace(StackClient.Role.prepConn, preparer)
  }

  case class Client(
    stack: Stack[ServiceFactory[ThriftClientRequest, Array[Byte]]] = Client.stack,
    params: Stack.Params = StackClient.defaultParams + ProtocolLibrary("thrift")
  ) extends StdStackClient[ThriftClientRequest, Array[Byte], Client]
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

    val param.Framed(framed) = params[param.Framed]
    protected val param.ProtocolFactory(protocolFactory) = params[param.ProtocolFactory]
    override protected lazy val Stats(stats) = params[Stats]

    protected def newTransporter(): Transporter[In, Out] = {
      val pipeline =
        if (framed) ThriftClientFramedPipelineFactory
        else ThriftClientBufferedPipelineFactory(protocolFactory)
      Netty3Transporter(pipeline, params)
    }

    protected def newDispatcher(
      transport: Transport[ThriftClientRequest, Array[Byte]]
    ): Service[ThriftClientRequest, Array[Byte]] =
      new SerialClientDispatcher(
        transport,
        params[Stats].statsReceiver.scope(GenSerialClientDispatcher.StatsScope)
      )

    def withProtocolFactory(protocolFactory: TProtocolFactory): Client =
      configured(param.ProtocolFactory(protocolFactory))

    def withClientId(clientId: thrift.ClientId): Client =
      configured(param.ClientId(Some(clientId)))

    def withAttemptTTwitterUpgrade: Client =
      configured(param.AttemptTTwitterUpgrade(true))

    def withNoAttemptTTwitterUpgrade: Client =
      configured(param.AttemptTTwitterUpgrade(false))

    def clientId: Option[thrift.ClientId] = params[param.ClientId].clientId

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
    override val withSession: SessionParams[Client] =
      new SessionParams(this)
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
        override val role = StackClient.Role.prepConn
        override val description = "Prepare TTwitter thrift connection"
        def make(params: Stack.Params, next: ServiceFactory[Array[Byte], Array[Byte]]) = {
          val Label(label) = params[Label]
          val param.ProtocolFactory(pf) = params[param.ProtocolFactory]
          val preparer = new thrift.ThriftServerPreparer(pf, label)
          preparer.prepare(next, params)
        }
    }

    val stack: Stack[ServiceFactory[Array[Byte], Array[Byte]]] = StackServer.newStack
      .replace(StackServer.Role.preparer, preparer)
  }

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

    val param.Framed(framed) = params[param.Framed]
    protected val param.ProtocolFactory(protocolFactory) = params[param.ProtocolFactory]

    protected def newListener(): Listener[In, Out] = {
      val pipeline =
        if (framed) thrift.ThriftServerFramedPipelineFactory
        else thrift.ThriftServerBufferedPipelineFactory(protocolFactory)

      Netty3Listener(pipeline,
        if (params.contains[Label]) params else params + Label("thrift"))
    }

    protected def newDispatcher(
      transport: Transport[In, Out],
      service: Service[Array[Byte], Array[Byte]]
    ) =
      new SerialServerDispatcher(transport, service)

    def withProtocolFactory(protocolFactory: TProtocolFactory): Server =
      configured(param.ProtocolFactory(protocolFactory))

    def withBufferedTransport(): Server =
      configured(param.Framed(false))

    // Java-friendly forwarders
    // See https://issues.scala-lang.org/browse/SI-8905
    override val withAdmissionControl: ServerAdmissionControlParams[Server] =
      new ServerAdmissionControlParams(this)
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

