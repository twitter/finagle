package com.twitter.finagle

import com.twitter.conversions.storage._
import com.twitter.finagle.client._
import com.twitter.finagle.naming.BindingFactory
import com.twitter.finagle.filter.PayloadSizeFilter
import com.twitter.finagle.liveness.FailureDetector
import com.twitter.finagle.mux.lease.exp.Lessor
import com.twitter.finagle.mux.transport._
import com.twitter.finagle.mux.{Handshake, Toggles}
import com.twitter.finagle.netty4.{Netty4Listener, Netty4Transporter}
import com.twitter.finagle.netty4.ssl.server.Netty4ServerSslHandler
import com.twitter.finagle.netty4.ssl.client.Netty4ClientSslHandler
import com.twitter.finagle.netty4.transport.ChannelTransport
import com.twitter.finagle.param.{ProtocolLibrary, WithDefaultLoadBalancer}
import com.twitter.finagle.pool.SingletonPool
import com.twitter.finagle.server._
import com.twitter.finagle.stats.{StatsReceiver, Counter}
import com.twitter.finagle.toggle.Toggle
import com.twitter.finagle.tracing._
import com.twitter.finagle.transport.{StatsTransport, Transport}
import com.twitter.finagle.{param => fparam}
import com.twitter.io.Buf
import com.twitter.logging.{Logger, Level}
import com.twitter.util.{Closable, Future, StorageUnit}
import io.netty.channel.{Channel, ChannelPipeline}
import java.net.SocketAddress

/**
 * A client and server for the mux protocol described in [[com.twitter.finagle.mux]].
 */
object Mux extends Client[mux.Request, mux.Response] with Server[mux.Request, mux.Response] {
  private val log = Logger.get

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
     * A class eligible for configuring if a client's TLS mode is opportunistic.
     * If it's not None, then mux will negotiate with the supplied level whether
     * to use TLS or not before setting up TLS.
     *
     * If it's None, it will not attempt to negotiate whether to use TLS or not
     * with the remote peer, and if TLS is configured, it will use mux over TLS.
     *
     * @note opportunistic TLS is not mutually intelligible with simple mux
     *       over TLS
     */
    case class OppTls(level: Option[OpportunisticTls.Level])
    object OppTls {
      implicit val param = Stack.Param(OppTls(None))
    }

    /**
     * A class eligible for configuring how to enable TLS.
     *
     * Only for internal use and testing--not intended to be exposed for
     * configuration to end-users.
     */
    private[finagle] case class TurnOnTlsFn(fn: (Stack.Params, ChannelPipeline) => Unit)
    private[finagle] object TurnOnTlsFn {
      implicit val param = Stack.Param(TurnOnTlsFn((_: Stack.Params, _: ChannelPipeline) => ()))
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
      transporter: Stack.Params => SocketAddress => Transporter[Buf, Buf, MuxContext],
      listener: Stack.Params => Listener[Buf, Buf, MuxContext]
    ) {
      def mk(): (MuxImpl, Stack.Param[MuxImpl]) =
        (this, MuxImpl.param)
    }

    // tells the Netty4Transporter not to turn on TLS so we can turn it on later
    private[this] def removeTlsIfOpportunisticClient(params: Stack.Params): Stack.Params = {
      val param.OppTls(level) = params[param.OppTls]
      level match {
        case None => params
        case _ => params + Transport.ClientSsl(None)
      }
    }

    // tells the Netty4Listener not to turn on TLS so we can turn it on later
    private[this] def removeTlsIfOpportunisticServer(params: Stack.Params): Stack.Params = {
      val param.OppTls(level) = params[param.OppTls]
      level match {
        case None => params
        case _ => params + Transport.ServerSsl(None)
      }
    }

    object MuxImpl {
      // exposed for testing
      private[finagle] val TlsHeadersToggleId: String = "com.twitter.finagle.mux.TlsHeaders"
      private val tlsHeadersToggle: Toggle[Int] = Toggles(TlsHeadersToggleId)
      private[finagle] def tlsHeaders: Boolean = tlsHeadersToggle(ServerInfo().id.hashCode)

      /**
       * A [[MuxImpl]] that uses netty4 as the underlying I/O multiplexer and
       * ref-counts inbound mux control messages. No application changes are
       * required to use this implementation.
       *
       * @note this is experimental and not yet tested in production.
       */
      val Netty4RefCountingControl = MuxImpl(
        params => {
          val MaxFrameSize(maxFrameSize) = params[MaxFrameSize]

          // there's no payload copy saved when dealing with fragmented
          // messages so revert to the copying decoder.
          if (maxFrameSize == Int.MaxValue.bytes)
            Netty4Transporter.raw(
              RefCountingFramer,
              _,
              removeTlsIfOpportunisticClient(params),
              transportFactory = { ch: Channel =>
                OpportunisticTls.transport(ch, params, new RefCountingTransport(ch))
              }
            )
          else {
            log.info(
              "disabled Netty4RefCountingControl decoder due to non-sentinel MaxFrameSize value"
            )
            Netty4Transporter.raw(
              CopyingFramer,
              _,
              removeTlsIfOpportunisticClient(params),
              transportFactory = { ch: Channel =>
                OpportunisticTls.transport(ch, params, new ChannelTransport(ch))
              }
            )
          }
        },
        params =>
          Netty4Listener(
            CopyingFramer,
            removeTlsIfOpportunisticServer(params),
            identity,
            transportFactory = { ch: Channel =>
              OpportunisticTls.transport(ch, params, new ChannelTransport(ch))
            }
        )
      )

      implicit val param = Stack.Param(Netty4RefCountingControl)
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
    statsReceiver: StatsReceiver,
    localEncryptLevel: OpportunisticTls.Level,
    turnOnTlsFn: () => Unit,
    upgrades: Counter
  ): Handshake.Negotiator = (peerHeaders, trans) => {
    import OpportunisticTls._

    val remoteMaxFrameSize = Handshake
      .valueOf(MuxFramer.Header.KeyBuf, peerHeaders)
      .map { cb =>
        MuxFramer.Header.decodeFrameSize(cb)
      }

    val remoteEncryptLevel = Handshake.valueOf(OpportunisticTls.Header.KeyBuf, peerHeaders) match {
      case Some(buf) => OpportunisticTls.Header.decodeLevel(buf)
      case None => Off
    }

    try {
      if (OpportunisticTls.negotiate(localEncryptLevel, remoteEncryptLevel)) {
        if (log.isLoggable(Level.DEBUG)) {
          log.debug(s"Successfully negotiated TLS with remote peer. " +
            s"local level: $localEncryptLevel, remote level: $remoteEncryptLevel")
        }
        upgrades.incr()
        turnOnTlsFn()
      }
    } catch {
      // TODO: handle IncompatibleNegotiationExceptions gracefully
      case exn: IncompatibleNegotiationException =>
        log.fatal(
          exn,
          s"The client wanted $localEncryptLevel and the server wanted" +
            s" $remoteEncryptLevel which are incompatible."
        )
        throw exn
    }

    // Decorate the transport with the MuxFramer. We need to handle the
    // cross product of local and remote configuration. The idea is that
    // both clients and servers can specify the maximum frame size they
    // would like their peer to send.
    val framerStats = statsReceiver.scope("framer")
    (maxFrameSize, remoteMaxFrameSize) match {
      // The remote peer has suggested a max frame size less than the
      // sentinel value. We need to configure the framer to fragment.
      case (_, s @ Some(remote)) if remote < Int.MaxValue =>
        MuxFramer(trans, s, framerStats)
      // The local instance has requested a max frame size less than the
      // sentinel value. We need to be prepared for the remote to send
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

  private[finagle] class ClientProtoTracing
      extends ProtoTracing("clnt", StackClient.Role.protoTracing)

  object Client {

    /** Prepends bound residual paths to outbound Mux requests's destinations. */
    private object MuxBindingFactory extends BindingFactory.Module[mux.Request, mux.Response] {
      protected[this] def boundPathFilter(residual: Path) =
        Filter.mk[mux.Request, mux.Response, mux.Request, mux.Response] { (req, service) =>
          service(mux.Request(residual ++ req.destination, Nil, req.body))
        }
    }

    private[finagle] val tlsEnable: (Stack.Params, ChannelPipeline) => Unit = (params, pipeline) =>
      pipeline.addFirst("opportunisticSslInit", new Netty4ClientSslHandler(params))

    private val params: Stack.Params = StackClient.defaultParams +
      ProtocolLibrary("mux") +
      param.TurnOnTlsFn(tlsEnable)

    private val stack: Stack[ServiceFactory[mux.Request, mux.Response]] = StackClient.newStack
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
    private[finagle] def headers(
      maxFrameSize: StorageUnit,
      tlsLevel: Option[OpportunisticTls.Level]
    ): Handshake.Headers = {
      val muxFrameHeader =
        MuxFramer.Header.KeyBuf -> MuxFramer.Header.encodeFrameSize(maxFrameSize.inBytes.toInt)
      tlsLevel match {
        case Some(level) => Seq(muxFrameHeader, OpportunisticTls.Header.KeyBuf -> level.buf)
        case _ => Seq(muxFrameHeader)
      }
    }
  }

  case class Client(
    stack: Stack[ServiceFactory[mux.Request, mux.Response]] = Client.stack,
    params: Stack.Params = Client.params
  ) extends StdStackClient[mux.Request, mux.Response, Client]
      with WithDefaultLoadBalancer[Client] {

    protected def copy1(
      stack: Stack[ServiceFactory[mux.Request, mux.Response]] = this.stack,
      params: Stack.Params = this.params
    ): Client = copy(stack, params)

    protected type In = Buf
    protected type Out = Buf
    protected type Context = MuxContext

    private[this] val statsReceiver = params[fparam.Stats].statsReceiver.scope("mux")

    protected def newTransporter(addr: SocketAddress): Transporter[In, Out, MuxContext] =
      params[param.MuxImpl].transporter(params)(addr)

    /**
     * Configures the client to negotiate whether to speak TLS or not.
     *
     * By default, the client doesn't use opportunistic TLS, and will instead try
     * to speak mux over TLS if TLS has been configured.
     *
     * The valid levels are Off, which indicates this client will never speak TLS,
     * Desired, which indicates it may speak TLS, but may also not speak TLS,
     * and Required, which indicates it must speak TLS.
     *
     * Clients that are configured to be Required cannot speak to servers that are
     * configured Off, and vice versa.
     *
     * Note that opportunistic TLS is negotiated in a cleartext handshake, and is
     * incompatible with mux over TLS.
     */
    def withOpportunisticTls(level: OpportunisticTls.Level): Client =
      configured(param.OppTls(Some(level)))

    /**
     * Disables oportunistic TLS.
     *
     * If the client is still TLS configured, it will speak mux over TLS.  To instead
     * configure the client to be `Off`, use `withOpportunisticTls(OpportunisticTls.Off)`.
     */
    def noOpportunisticTls: Client = configured(param.OppTls(None))

    protected def newDispatcher(
      transport: Transport[In, Out] { type Context <: Client.this.Context }
    ): Service[mux.Request, mux.Response] = {
      val FailureDetector.Param(detectorConfig) = params[FailureDetector.Param]
      val fparam.ExceptionStatsHandler(excRecorder) = params[fparam.ExceptionStatsHandler]
      val fparam.Label(name) = params[fparam.Label]
      val param.MaxFrameSize(maxFrameSize) = params[param.MaxFrameSize]
      val param.OppTls(level) = params[param.OppTls]
      val upgrades = statsReceiver.counter("tls", "upgrade", "success")

      val negotiatedTrans = mux.Handshake.client(
        trans = transport,
        version = LatestVersion,
        headers = Client.headers(
          maxFrameSize,
          if (param.MuxImpl.tlsHeaders) level.orElse(Some(OpportunisticTls.Off)) else None),
        negotiate = negotiate(
          maxFrameSize,
          statsReceiver,
          level.getOrElse(OpportunisticTls.Off),
          transport.context.turnOnTls _,
          upgrades
        )
      )

      val statsTrans =
        new StatsTransport(negotiatedTrans, excRecorder, statsReceiver.scope("transport"))

      val session = new mux.ClientSession(statsTrans, detectorConfig, name, statsReceiver)

      mux.ClientDispatcher.newRequestResponse(session)
    }
  }

  def client: Mux.Client = Client()

  def newService(dest: Name, label: String): Service[mux.Request, mux.Response] =
    client.newService(dest, label)

  def newClient(dest: Name, label: String): ServiceFactory[mux.Request, mux.Response] =
    client.newClient(dest, label)

  private[finagle] class ServerProtoTracing
      extends ProtoTracing("srv", StackServer.Role.protoTracing)

  object Server {
    private val stack: Stack[ServiceFactory[mux.Request, mux.Response]] = StackServer.newStack
      .remove(TraceInitializerFilter.role)
      .replace(StackServer.Role.protoTracing, new ServerProtoTracing)
      .prepend(PayloadSizeFilter.module(_.body.length, _.body.length))

    private[finagle] val tlsEnable: (Stack.Params, ChannelPipeline) => Unit = (params, pipeline) =>
      pipeline.addFirst("opportunisticSslInit", new Netty4ServerSslHandler(params))

    private val params: Stack.Params = StackServer.defaultParams +
      ProtocolLibrary("mux") +
      param.TurnOnTlsFn(tlsEnable)

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
      maxFrameSize: StorageUnit,
      tlsLevel: Option[OpportunisticTls.Level]
    ): Handshake.Headers = {
      val muxFrameHeader =
        MuxFramer.Header.KeyBuf -> MuxFramer.Header.encodeFrameSize(maxFrameSize.inBytes.toInt)

      tlsLevel match {
        case Some(level) => Seq(muxFrameHeader, OpportunisticTls.Header.KeyBuf -> level.buf)
        case _ => Seq(muxFrameHeader)
      }
    }
  }

  case class Server(
    stack: Stack[ServiceFactory[mux.Request, mux.Response]] = Server.stack,
    params: Stack.Params = Server.params
  ) extends StdStackServer[mux.Request, mux.Response, Server] {

    protected def copy1(
      stack: Stack[ServiceFactory[mux.Request, mux.Response]] = this.stack,
      params: Stack.Params = this.params
    ): Server = copy(stack, params)

    protected type In = Buf
    protected type Out = Buf
    protected type Context = MuxContext

    private[this] val statsReceiver = params[fparam.Stats].statsReceiver.scope("mux")

    /**
     * Configures the server to negotiate whether to speak TLS or not.
     *
     * By default, the server doesn't use opportunistic TLS, and will instead try
     * to speak mux over TLS if TLS has been configured.
     *
     * The valid levels are Off, which indicates this server will never speak TLS,
     * Desired, which indicates it may speak TLS, but may also not speak TLS,
     * and Required, which indicates it must speak TLS.
     *
     * Servers that are configured to be Required cannot speak to clients that are
     * configured Off, and vice versa.
     *
     * Note that opportunistic TLS is negotiated in a cleartext handshake, and is
     * incompatible with mux over TLS.
     */
    def withOpportunisticTls(level: OpportunisticTls.Level): Server =
      configured(param.OppTls(Some(level)))

    /**
     * Disables oportunistic TLS.
     *
     * If the server is still TLS configured, it will speak mux over TLS.  To instead
     * configure the server to be `Off`, use `withOpportunisticTls(OpportunisticTls.Off)`.
     */
    def noOpportunisticTls: Server = configured(param.OppTls(None))

    protected def newListener(): Listener[In, Out, MuxContext] =
      params[param.MuxImpl].listener(params)

    // we cache tlsHeaders here because it's hard to do a let on servers
    private[this] val cachedTlsHeaders = param.MuxImpl.tlsHeaders

    protected def newDispatcher(
      transport: Transport[In, Out] { type Context <: Server.this.Context },
      service: Service[mux.Request, mux.Response]
    ): Closable = {
      val fparam.Tracer(tracer) = params[fparam.Tracer]
      val Lessor.Param(lessor) = params[Lessor.Param]
      val fparam.ExceptionStatsHandler(excRecorder) = params[fparam.ExceptionStatsHandler]
      val param.MaxFrameSize(maxFrameSize) = params[param.MaxFrameSize]
      val param.OppTls(level) = params[param.OppTls]
      val upgrades = statsReceiver.counter("tls", "upgrade", "success")

      val negotiatedTrans = mux.Handshake.server(
        trans = transport,
        version = LatestVersion,
        headers = Server.headers(
          _,
          maxFrameSize,
          if (cachedTlsHeaders) level.orElse(Some(OpportunisticTls.Off)) else None),
        negotiate = negotiate(
          maxFrameSize,
          statsReceiver,
          level.getOrElse(OpportunisticTls.Off),
          transport.context.turnOnTls _,
          upgrades
        )
      )

      val statsTrans =
        new StatsTransport(negotiatedTrans, excRecorder, statsReceiver.scope("transport"))

      mux.ServerDispatcher.newRequestResponse(statsTrans, service, lessor, tracer, statsReceiver)
    }
  }

  def server: Mux.Server = Server()

  def serve(
    addr: SocketAddress,
    service: ServiceFactory[mux.Request, mux.Response]
  ): ListeningServer = server.serve(addr, service)
}
