package com.twitter.finagle

import com.twitter.finagle.client.{StackClient, StackBasedClient}
import com.twitter.finagle.mux.transport.Message
import com.twitter.finagle.netty3.Netty3Listener
import com.twitter.finagle.param.{Label, Stats, ProtocolLibrary}
import com.twitter.finagle.server.{StackBasedServer, Listener, StackServer, StdStackServer}
import com.twitter.finagle.service.RetryPolicy
import com.twitter.finagle.Stack.Param
import com.twitter.finagle.stats.{ClientStatsReceiver, ServerStatsReceiver}
import com.twitter.finagle.thrift.{ClientId, ThriftClientRequest, UncaughtAppExceptionFilter}
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.transport.Transport
import com.twitter.io.Buf
import com.twitter.util.{Closable, Future, NonFatal}
import java.net.SocketAddress
import org.apache.thrift.protocol.TProtocolFactory
import org.apache.thrift.TException
import org.apache.thrift.transport.TMemoryInputTransport
import org.jboss.netty.buffer.ChannelBuffer

/**
 * The `ThriftMux` object is both a [[com.twitter.finagle.Client]] and a
 * [[com.twitter.finagle.Server]] for the Thrift protocol served over
 * [[com.twitter.finagle.mux]]. Rich interfaces are provided to adhere to those
 * generated from a [[http://thrift.apache.org/docs/idl/ Thrift IDL]] by
 * [[http://twitter.github.io/scrooge/ Scrooge]] or
 * [[https://github.com/mariusaeriksen/thrift-0.5.0-finagle thrift-finagle]].
 *
 * Clients can be created directly from an interface generated from
 * a Thrift IDL:
 *
 * $clientExample
 *
 * Servers are also simple to expose:
 *
 * $serverExample
 *
 * This object does not expose any configuration options. Both clients and servers
 * are instantiated with sane defaults. Clients are labeled with the "clnt/thrift"
 * prefix and servers with "srv/thrift". If you'd like more configuration, see the
 * [[com.twitter.finagle.ThriftMux.Server]] and [[com.twitter.finagle.ThriftMux.Client]]
 * classes.
 *
 * @define clientExampleObject ThriftMux
 * @define serverExampleObject ThriftMux
 */
object ThriftMux
  extends Client[ThriftClientRequest, Array[Byte]] with ThriftRichClient
  with Server[Array[Byte], Array[Byte]] with ThriftRichServer
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

    override def make(next: ServiceFactory[mux.Request, mux.Response]) =
      rpcTracer andThen super.make(next)
  }

  case class Client(
      muxer: StackClient[mux.Request, mux.Response] = Mux.client.copy(stack = BaseClientStack)
        .configured(ProtocolLibrary("thriftmux")))
    extends StackBasedClient[ThriftClientRequest, Array[Byte]]
    with Stack.Parameterized[Client]
    with Stack.Transformable[Client]
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

    private[this] val Thrift.param.ClientId(clientId) = params[Thrift.param.ClientId]

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

    def newService(dest: Name, label: String): Service[ThriftClientRequest, Array[Byte]] =
      ThriftMuxToMux andThen muxer.newService(dest, label)

    def newClient(dest: Name, label: String): ServiceFactory[ThriftClientRequest, Array[Byte]] =
      ThriftMuxToMux andThen muxer.newClient(dest, label)

    // these are necessary to have the right types from Java
    override def configured[P: Stack.Param](p: P): Client = super.configured(p)
    override def configured[P](psp: (P, Stack.Param[P])): Client = super.configured(psp)
  }

  val client = Client()
    .configured(Label("thrift"))
    .configured(Stats(ClientStatsReceiver))

  protected lazy val Label(defaultClientName) = client.params[Label]

  override protected lazy val Stats(stats) = client.params[Stats]

  protected val Thrift.param.ProtocolFactory(protocolFactory) =
    client.params[Thrift.param.ProtocolFactory]

  def newClient(dest: Name, label: String) = client.newClient(dest, label)
  def newService(dest: Name, label: String) = client.newService(dest, label)

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
    params: Stack.Params = Mux.server.params + ProtocolLibrary("thriftmux")
  ) extends StdStackServer[mux.Request, mux.Response, ServerMuxer] {

    protected type In = ChannelBuffer
    protected type Out = ChannelBuffer

    private[this] val muxStatsReceiver = {
      val Stats(statsReceiver) = params[Stats]
      statsReceiver.scope("mux")
    }

    protected def copy1(
      stack: Stack[ServiceFactory[mux.Request, mux.Response]] = this.stack,
      params: Stack.Params = this.params
    ) = copy(stack, params)

    protected def newListener(): Listener[In, Out] = {
      val Stats(sr) = params[Stats]
      val scoped = sr.scope("thriftmux")
      val Thrift.param.ProtocolFactory(pf) = params[Thrift.param.ProtocolFactory]

      // Create a Listener with a pipeline that can downgrade the connection
      // to vanilla thrift.
      new Listener[In, Out] {
        private[this] val underlying = Netty3Listener[In, Out](
          new thriftmux.PipelineFactory(scoped, pf),
          params
        )

        def listen(addr: SocketAddress)(
          serveTransport: Transport[In, Out] => Unit
        ): ListeningServer = underlying.listen(addr)(serveTransport)
      }
    }

    protected def newDispatcher(
      transport: Transport[In, Out],
      service: Service[mux.Request, mux.Response]
    ): Closable = {
      val param.Tracer(tracer) = params[param.Tracer]
      mux.ServerDispatcher.newRequestResponse(
        transport.map(Message.encode, Message.decode), service,
        mux.lease.exp.ClockedDrainer.flagged,
        tracer, muxStatsReceiver)
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

    private[this] class ExnFilter(protocolFactory: TProtocolFactory)
      extends SimpleFilter[mux.Request, mux.Response]
    {
      def apply(
        request: mux.Request,
        service: Service[mux.Request, mux.Response]
      ): Future[mux.Response] =
        service(request).rescue {
          case e@RetryPolicy.RetryableWriteException(_) =>
            Future.exception(e)
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
      ) = {
        val Thrift.param.ProtocolFactory(pf) = _pf
        val exnFilter = new ExnFilter(pf)
        exnFilter.andThen(next)
      }
    }
  }

  case class Server(
      muxer: StackServer[mux.Request, mux.Response] = serverMuxer)
    extends StackBasedServer[Array[Byte], Array[Byte]]
    with ThriftRichServer
    with Stack.Parameterized[Server]
  {
    import Server.MuxToArrayFilter

    def stack: Stack[ServiceFactory[mux.Request, mux.Response]] =
      muxer.stack

    override protected val Label(serverLabel) = params[Label]

    override protected lazy val Stats(serverStats) = params[Stats]

    def params: Stack.Params = muxer.params

    override def configured[P](psp: (P, Param[P])): Server =
      super.configured(psp)

    override def configured[P: Param](p: P): Server =
      super.configured(p)

    protected val Thrift.param.ProtocolFactory(protocolFactory) =
      params[Thrift.param.ProtocolFactory]

    override val Thrift.param.MaxReusableBufferSize(maxThriftBufferSize) =
      params[Thrift.param.MaxReusableBufferSize]

    /**
     * Produce a [[com.twitter.finagle.Thrift.Server]] using the provided
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
      configured(Thrift.param.MaxReusableBufferSize(size))

    def withParams(ps: Stack.Params): Server =
      copy(muxer = muxer.withParams(ps))

    private[this] val tracingFilter = new SimpleFilter[Array[Byte], Array[Byte]] {
      def apply(request: Array[Byte], svc: Service[Array[Byte], Array[Byte]]): Future[Array[Byte]] = {
        recordRpc(request)
        svc(request)
      }
    }

    def serve(addr: SocketAddress, factory: ServiceFactory[Array[Byte], Array[Byte]]) = {
      muxer.serve(
        addr,
        MuxToArrayFilter.andThen(tracingFilter).andThen(factory))
    }
  }

  val server: Server = Server()
    .configured(Label("thrift"))
    .configured(Stats(ServerStatsReceiver))

  def serve(addr: SocketAddress, factory: ServiceFactory[Array[Byte], Array[Byte]]) =
    server.serve(addr, factory)
}
