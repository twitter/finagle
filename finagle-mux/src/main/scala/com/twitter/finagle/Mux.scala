package com.twitter.finagle

import com.twitter.conversions.storage._
import com.twitter.finagle.client._
import com.twitter.finagle.factory.BindingFactory
import com.twitter.finagle.filter.PayloadSizeFilter
import com.twitter.finagle.mux.lease.exp.Lessor
import com.twitter.finagle.mux.transport.{Message, MuxFramer, Netty3Framer, Netty4Framer}
import com.twitter.finagle.mux.{FailureDetector, Handshake, Toggles}
import com.twitter.finagle.netty3.{Netty3Listener, Netty3Transporter}
import com.twitter.finagle.netty4.{Netty4Listener, Netty4Transporter}
import com.twitter.finagle.param.{ProtocolLibrary, WithDefaultLoadBalancer}
import com.twitter.finagle.pool.SingletonPool
import com.twitter.finagle.server._
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.toggle.Toggle
import com.twitter.finagle.tracing._
import com.twitter.finagle.transport.{StatsTransport, Transport}
import com.twitter.finagle.{param => fparam}
import com.twitter.io.Buf
import com.twitter.util.{Closable, Future, StorageUnit}
import java.net.SocketAddress

/**
 * A client and server for the mux protocol described in [[com.twitter.finagle.mux]].
 */
object Mux extends Client[mux.Request, mux.Response] with Server[mux.Request, mux.Response] {
  /**
   * The current version of the mux protocol.
   */
  val LatestVersion: Short = 0x0001

  /**
   * Mux-specific stack params.
   */
  object param {
    /**
     * A class eligible for configuring the maximum size of a mux frame.
     * Any message that is larger than this value is fragmented across multiple
     * transmissions. Clients and Servers can use this to set an upper bound
     * on the size of messages they are willing to receive. The value is exchanged
     * and applied during the mux handshake.
     */
    case class MaxFrameSize(size: StorageUnit) {
      assert(size.inBytes <= Int.MaxValue, s"$size is not <= Int.MaxValue bytes")
      assert(size.inBytes > 0, s"$size must be positive")

      def mk(): (MaxFrameSize, Stack.Param[MaxFrameSize]) =
        (this, MaxFrameSize.param)
    }
    object MaxFrameSize {
      implicit val param = Stack.Param(MaxFrameSize(Int.MaxValue.bytes))
    }

    /**
     * A param that controls the [[Transporter]] and [[Listener]] implementation
     * used by Mux. This allows us to easily swap the underlying I/O multiplexer
     * implementation.
     *
     * @note the listener and transporter don't strictly need to be
     * coupled but we do so for ease of configuration (e.g. both
     * servers and clients can use the same parameter).
     */
    case class MuxImpl(
        transporter: Stack.Params => Transporter[Buf, Buf],
        listener: Stack.Params => Listener[Buf, Buf]) {
      def mk(): (MuxImpl, Stack.Param[MuxImpl]) =
        (this, MuxImpl.param)
    }

    object MuxImpl {
      private val UseNetty4ToggleId: String = "com.twitter.finagle.mux.UseNetty4"
      private val netty4Toggle: Toggle[Int] = Toggles(UseNetty4ToggleId)
      private def useNetty4: Boolean = netty4Toggle(ServerInfo().id.hashCode)

      /**
       * A [[MuxImpl]] that uses netty3 as the underlying I/O multiplexer.
       */
      val Netty3 = MuxImpl(
        params => Netty3Transporter(Netty3Framer, params),
        params => Netty3Listener(Netty3Framer, params))

      /**
       * A [[MuxImpl]] that uses netty4 as the underlying I/O multiplexer.
       *
       * @note this is experimental and not yet tested in production.
       */
      val Netty4 = MuxImpl(
        params => Netty4Transporter(Netty4Framer, params),
        params => Netty4Listener(Netty4Framer, params))


      implicit val param = Stack.Param(
        if (useNetty4) Netty4
        else Netty3
      )
    }
  }

  /**
   * Extract feature flags from peer headers and decorate the trans.
   *
   * @param maxFrameSize the maximum frame size that was sent to the peer.
   *
   * @param statsReceiver the stats receiver used to configure various modules
   * configured during negotiation.
   */
  private[finagle] def negotiate(
    maxFrameSize: StorageUnit,
    statsReceiver: StatsReceiver
  ): Handshake.Negotiator = (peerHeaders, trans) => {
    val remoteMaxFrameSize = Handshake.valueOf(MuxFramer.Header.KeyBuf, peerHeaders)
      .map { cb => MuxFramer.Header.decodeFrameSize(cb) }
    // Decorate the transport with the MuxFramer. We need to handle the
    // cross product of local and remote configuration. The idea is that
    // both clients and servers can specify the maximum frame size they
    // would like their peer to send.
    val framerStats = statsReceiver.scope("framer")
    (maxFrameSize, remoteMaxFrameSize) match {
      // The remote peer has suggested a max frame size less than the
      // sentinal value. We need to configure the framer to fragment.
      case (_, s@Some(remote)) if remote < Int.MaxValue =>
        MuxFramer(trans, s, framerStats)
      // The local instance has requested a max frame size less than the
      // sentinal value. We need to be prepared for the remote to send
      // fragments.
      case (local, _) if local.inBytes < Int.MaxValue =>
        MuxFramer(trans, None, framerStats)
      case (_, _) => trans.map(Message.encode, Message.decode)
    }
  }

  private[finagle] abstract class ProtoTracing(
    process: String,
    val role: Stack.Role
  ) extends Stack.Module0[ServiceFactory[mux.Request, mux.Response]] {
    val description = s"Mux specific $process traces"

    private[this] val tracingFilter = new SimpleFilter[mux.Request, mux.Response] {
      def apply(req: mux.Request, svc: Service[mux.Request, mux.Response]): Future[mux.Response] = {
        Trace.recordBinary(s"$process/mux/enabled", true)
        svc(req)
      }
    }

    def make(next: ServiceFactory[mux.Request, mux.Response]) =
      tracingFilter andThen next
  }

  private[finagle] class ClientProtoTracing extends ProtoTracing("clnt", StackClient.Role.protoTracing)

  object Client {
    /** Prepends bound residual paths to outbound Mux requests's destinations. */
    private object MuxBindingFactory extends BindingFactory.Module[mux.Request, mux.Response] {
      protected[this] def boundPathFilter(residual: Path) =
        Filter.mk[mux.Request, mux.Response, mux.Request, mux.Response] { (req, service) =>
          service(mux.Request(residual ++ req.destination, req.body))
        }
    }

    val stack: Stack[ServiceFactory[mux.Request, mux.Response]] = StackClient.newStack
      .replace(StackClient.Role.pool, SingletonPool.module[mux.Request, mux.Response])
      .replace(StackClient.Role.protoTracing, new ClientProtoTracing)
      .replace(BindingFactory.role, MuxBindingFactory)
      .prepend(PayloadSizeFilter.module(_.body.length, _.body.length))

    /**
     * Returns the headers that a client sends to a server.
     *
     * @param maxFrameSize the maximum mux fragment size the client is willing to
     * receive from a server.
     */
    private def headers(maxFrameSize: StorageUnit): Handshake.Headers = Seq(
      MuxFramer.Header.KeyBuf -> MuxFramer.Header.encodeFrameSize(
        maxFrameSize.inBytes.toInt)
    )
  }

  case class Client(
      stack: Stack[ServiceFactory[mux.Request, mux.Response]] = Client.stack,
      params: Stack.Params = StackClient.defaultParams + ProtocolLibrary("mux"))
    extends StdStackClient[mux.Request, mux.Response, Client]
    with WithDefaultLoadBalancer[Client] {

    protected def copy1(
      stack: Stack[ServiceFactory[mux.Request, mux.Response]] = this.stack,
      params: Stack.Params = this.params
    ): Client = copy(stack, params)

    protected type In = Buf
    protected type Out = Buf

    private[this] val statsReceiver = params[fparam.Stats].statsReceiver.scope("mux")

    protected def newTransporter(): Transporter[In, Out] =
      params[param.MuxImpl].transporter(params)

    protected def newDispatcher(
      transport: Transport[In, Out]
    ): Service[mux.Request, mux.Response] = {
      val FailureDetector.Param(detectorConfig) = params[FailureDetector.Param]
      val fparam.ExceptionStatsHandler(excRecorder) = params[fparam.ExceptionStatsHandler]
      val fparam.Label(name) = params[fparam.Label]
      val param.MaxFrameSize(maxFrameSize) = params[param.MaxFrameSize]

      val negotiatedTrans = mux.Handshake.client(
        trans = transport,
        version = LatestVersion,
        headers = Client.headers(maxFrameSize),
        negotiate = negotiate(maxFrameSize, statsReceiver))

      val statsTrans = new StatsTransport(
        negotiatedTrans,
        excRecorder,
        statsReceiver.scope("transport"))

      val session = new mux.ClientSession(
        statsTrans,
        detectorConfig,
        name,
        statsReceiver)

      mux.ClientDispatcher.newRequestResponse(session)
    }
  }

  val client = Client()

  def newService(dest: Name, label: String): Service[mux.Request, mux.Response] =
    client.newService(dest, label)

  def newClient(dest: Name, label: String): ServiceFactory[mux.Request, mux.Response] =
    client.newClient(dest, label)

  private[finagle] class ServerProtoTracing extends ProtoTracing("srv", StackServer.Role.protoTracing)

  object Server {
    val stack: Stack[ServiceFactory[mux.Request, mux.Response]] = StackServer.newStack
      .remove(TraceInitializerFilter.role)
      .replace(StackServer.Role.protoTracing, new ServerProtoTracing)
      .prepend(PayloadSizeFilter.module(_.body.length, _.body.length))

    /**
     * Returns the headers that a server sends to a client.
     *
     * @param clientHeaders The headers received from the client. This is useful since
     * the headers the server responds with can be based on the clients.
     *
     * @param maxFrameSize the maximum mux fragment size the server is willing to
     * receive from a client.
     */
    private[finagle] def headers(
      clientHeaders: Handshake.Headers,
      maxFrameSize: StorageUnit
    ): Handshake.Headers = {
      Seq(MuxFramer.Header.KeyBuf -> MuxFramer.Header.encodeFrameSize(
        maxFrameSize.inBytes.toInt))
    }
  }

  case class Server(
      stack: Stack[ServiceFactory[mux.Request, mux.Response]] = Server.stack,
      params: Stack.Params = StackServer.defaultParams + ProtocolLibrary("mux"))
    extends StdStackServer[mux.Request, mux.Response, Server] {

    protected def copy1(
      stack: Stack[ServiceFactory[mux.Request, mux.Response]] = this.stack,
      params: Stack.Params = this.params
    ): Server = copy(stack, params)

    protected type In = Buf
    protected type Out = Buf

    private[this] val statsReceiver = params[fparam.Stats].statsReceiver.scope("mux")

    protected def newListener(): Listener[In, Out] =
      params[param.MuxImpl].listener(params)

    protected def newDispatcher(
      transport: Transport[In, Out],
      service: Service[mux.Request, mux.Response]
    ): Closable = {
      val fparam.Tracer(tracer) = params[fparam.Tracer]
      val Lessor.Param(lessor) = params[Lessor.Param]
      val fparam.ExceptionStatsHandler(excRecorder) = params[fparam.ExceptionStatsHandler]
      val param.MaxFrameSize(maxFrameSize) = params[param.MaxFrameSize]

      val negotiatedTrans = mux.Handshake.server(
        trans = transport,
        version = LatestVersion,
        headers = Server.headers(_, maxFrameSize),
        negotiate = negotiate(maxFrameSize, statsReceiver))

      val statsTrans = new StatsTransport(
        negotiatedTrans,
        excRecorder,
        statsReceiver.scope("transport"))

      mux.ServerDispatcher.newRequestResponse(
        statsTrans,
        service,
        lessor,
        tracer,
        statsReceiver)
    }
  }

  val server = Server()

  def serve(
    addr: SocketAddress,
    service: ServiceFactory[mux.Request, mux.Response]
  ): ListeningServer = server.serve(addr, service)
}
