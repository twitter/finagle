package com.twitter.finagle

import com.twitter.conversions.StorageUnitOps._
import com.twitter.finagle.Mux.param.CompressionPreferences
import com.twitter.finagle.Mux.param.MaxFrameSize
import com.twitter.finagle.client._
import com.twitter.finagle.factory.TimeoutFactory
import com.twitter.finagle.naming.BindingFactory
import com.twitter.finagle.filter.NackAdmissionFilter
import com.twitter.finagle.filter.PayloadSizeFilter
import com.twitter.finagle.mux.Handshake.Headers
import com.twitter.finagle.mux.pushsession._
import com.twitter.finagle.mux.transport.{OpportunisticTls => MuxOpportunisticTls, _}
import com.twitter.finagle.mux.ExportCompressionUsage
import com.twitter.finagle.mux.Handshake
import com.twitter.finagle.mux.OpportunisticTlsParams
import com.twitter.finagle.mux.Request
import com.twitter.finagle.mux.Response
import com.twitter.finagle.mux.WithCompressionPreferences
import com.twitter.finagle.netty4.pushsession.Netty4PushListener
import com.twitter.finagle.netty4.pushsession.Netty4PushTransporter
import com.twitter.finagle.netty4.ssl.server.Netty4ServerSslChannelInitializer
import com.twitter.finagle.netty4.ssl.client.Netty4ClientSslChannelInitializer
import com.twitter.finagle.param.Label
import com.twitter.finagle.param.OppTls
import com.twitter.finagle.param.ProtocolLibrary
import com.twitter.finagle.param.Stats
import com.twitter.finagle.param.Timer
import com.twitter.finagle.param.WithDefaultLoadBalancer
import com.twitter.finagle.pool.BalancingPool
import com.twitter.finagle.pushsession._
import com.twitter.finagle.server._
import com.twitter.finagle.ssl.OpportunisticTls
import com.twitter.finagle.ssl.SnoopingLevelInterpreter
import com.twitter.finagle.tracing._
import com.twitter.finagle.transport.Transport.ClientSsl
import com.twitter.finagle.transport.Transport.ServerSsl
import com.twitter.finagle.transport.Transport
import com.twitter.io.Buf
import com.twitter.io.ByteReader
import com.twitter.util.Future
import com.twitter.util.StorageUnit
import io.netty.channel.Channel
import io.netty.channel.ChannelPipeline
import java.net.SocketAddress
import java.util.concurrent.Executor
import scala.collection.mutable.ArrayBuffer

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
    @deprecated("Please use com.twitter.finagle.param.OppTls directly", "2021-03-11")
    type OppTls = com.twitter.finagle.param.OppTls

    @deprecated("Please use com.twitter.finagle.param.OppTls directly", "2021-03-11")
    object OppTls {
      def apply(level: Option[OpportunisticTls.Level]): OppTls =
        com.twitter.finagle.param.OppTls(level)

      /** Determine whether opportunistic TLS is configured to `Desired` or `Required`. */
      def enabled(params: Stack.Params): Boolean = com.twitter.finagle.param.OppTls.enabled(params)
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

    // tells the Netty4Transporter not to turn on TLS so we can turn it on later
    private[finagle] def removeTlsIfOpportunisticClient(params: Stack.Params): Stack.Params = {
      params[OppTls].level match {
        case None => params
        case _ => params + Transport.ClientSsl(None)
      }
    }

    // tells the Netty4Listener not to turn on TLS so we can turn it on later
    private[finagle] def removeTlsIfOpportunisticServer(params: Stack.Params): Stack.Params = {
      // If we have snooping enabled we're going to leave the params as is.
      // The snooper will decided whether to add the TLS handler.
      if (SnoopingLevelInterpreter.shouldEnableSnooping(params)) params
      else {
        params[OppTls].level match {
          case None => params
          case _ => params + Transport.ServerSsl(None)
        }
      }
    }

    private[finagle] case class PingManager(builder: (Executor, MessageWriter) => ServerPingManager)

    private[finagle] object PingManager {
      implicit val param = Stack.Param(PingManager { (_, writer) =>
        ServerPingManager.default(writer)
      })
    }

    /**
     * A class eligible for configuring if the client or server is willing to compress or decompress
     * requests or responses.
     */
    case class CompressionPreferences(compressionPreferences: Compression.LocalPreferences) {
      def mk(): (CompressionPreferences, Stack.Param[CompressionPreferences]) =
        (this, CompressionPreferences.param)
    }
    object CompressionPreferences {
      implicit val param = Stack.Param(CompressionPreferences(Compression.DefaultLocal))
    }
  }

  object Client {

    /** Prepends bound residual paths to outbound Mux requests's destinations. */
    private object MuxBindingFactory extends BindingFactory.Module[mux.Request, mux.Response] {
      protected[this] def boundPathFilter(residual: Path) =
        Filter.mk[mux.Request, mux.Response, mux.Request, mux.Response] { (req, service) =>
          service(mux.Request(residual ++ req.destination, req.contexts, req.body))
        }
    }

    private[finagle] val tlsEnable: (Stack.Params, ChannelPipeline) => Unit = (params, pipeline) =>
      pipeline.addFirst("opportunisticSslInit", new Netty4ClientSslChannelInitializer(params))

    private[finagle] def params: Stack.Params = StackClient.defaultParams +
      ProtocolLibrary("mux") +
      param.TurnOnTlsFn(tlsEnable)

    private val stack: Stack[ServiceFactory[mux.Request, mux.Response]] = StackClient.newStack
    // We use a singleton pool to manage a multiplexed session. Because it's mux'd, we
    // don't want arbitrary interrupts on individual dispatches to cancel outstanding service
    // acquisitions, so we disable `allowInterrupts`.
      .replace(
        StackClient.Role.pool,
        BalancingPool.module[mux.Request, mux.Response](allowInterrupts = false)
      )
      // As per the config above, we don't allow interrupts to propagate past the pool.
      // However, we need to provide a way to cancel service acquisitions which are taking
      // too long, so we "move" the [[TimeoutFactory]] below the pool.
      .remove(StackClient.Role.postNameResolutionTimeout)
      .insertAfter(
        StackClient.Role.pool,
        TimeoutFactory.module[mux.Request, mux.Response](Stack.Role("MuxSessionTimeout"))
      )
      .replace(BindingFactory.role, MuxBindingFactory)
      // Because the payload filter also traces the sizes, it's important that we do so
      // after the tracing context is initialized.
      .insertAfter(
        TraceInitializerFilter.role,
        PayloadSizeFilter.clientModule[mux.Request, mux.Response](_.body.length, _.body.length)
      )
      // Since NackAdmissionFilter should operate on all requests sent over
      // the wire including retries, it must be below `Retries`. Since it
      // aggregates the status of the entire cluster, it must be above
      // `LoadBalancerFactory` (not part of the endpoint stack).
      .insertBefore(
        StackClient.Role.prepFactory,
        NackAdmissionFilter.module[mux.Request, mux.Response]
      )
      .prepend(ExportCompressionUsage.module)

    /**
     * Returns the headers that a client sends to a server.
     *
     * @param maxFrameSize the maximum mux fragment size the client is willing to
     * receive from a server.
     */
    private[finagle] def headers(
      maxFrameSize: StorageUnit,
      tlsLevel: OpportunisticTls.Level,
      compressionPreferences: Compression.LocalPreferences
    ): Handshake.Headers = {
      val buffer = ArrayBuffer(
        MuxFramer.Header.KeyBuf -> MuxFramer.Header.encodeFrameSize(maxFrameSize.inBytes.toInt),
        MuxOpportunisticTls.Header.KeyBuf -> tlsLevel.buf
      )

      if (!compressionPreferences.isDisabled) {
        buffer += (CompressionNegotiation.ClientHeader.KeyBuf ->
          CompressionNegotiation.ClientHeader.encode(compressionPreferences))
      }
      buffer.toSeq
    }

    /**
     * Check the opportunistic TLS configuration to ensure it's in a consistent state
     */
    private def validateTlsParamConsistency(dest: Name, params: Stack.Params): Unit = {
      if (OppTls.enabled(params) && params[ClientSsl].sslClientConfiguration.isEmpty) {
        val level = params[OppTls].level
        val label = params[Label].label
        throw new IllegalStateException(
          s"Client($label) with destination ${Name.showable.show(dest)} and desired opportunistic " +
            s"TLS ($level) is missing the SSL configuration. Make sure that your TLS settings " +
            s"are properly configured."
        )
      }
    }
  }

  final case class Client(
    stack: Stack[ServiceFactory[mux.Request, mux.Response]] = Mux.Client.stack,
    params: Stack.Params = Mux.Client.params)
      extends PushStackClient[mux.Request, mux.Response, Client]
      with WithDefaultLoadBalancer[Client]
      with OpportunisticTlsParams[Client]
      with WithCompressionPreferences[Client] {

    // These are lazy because the stats receiver is not appropriately scoped
    // until after the `.newService` call (or equivalent). If they're not lazy
    // we'll end up with dead stats scoped to `mux/...` instead of `client_name/mux/...`.
    private[this] lazy val statsReceiver = params[Stats].statsReceiver
    private[this] lazy val sessionStats = new SharedNegotiationStats(statsReceiver)
    private[this] lazy val sessionParams = params + Stats(statsReceiver.scope("mux"))

    protected type SessionT = MuxClientNegotiatingSession
    protected type In = ByteReader
    protected type Out = Buf

    protected def newSession(
      handle: PushChannelHandle[ByteReader, Buf]
    ): Future[MuxClientNegotiatingSession] = {
      val negotiator: Option[Headers] => Future[MuxClientSession] = { headers =>
        new Negotiation.Client(sessionParams, sessionStats).negotiateAsync(handle, headers)
      }
      val headers = Mux.Client.headers(
        params[MaxFrameSize].size,
        params[OppTls].level.getOrElse(OpportunisticTls.Off),
        params[CompressionPreferences].compressionPreferences
      )

      Future.value(
        new MuxClientNegotiatingSession(
          handle = handle,
          version = Mux.LatestVersion,
          negotiator = negotiator,
          headers = headers,
          name = params[Label].label,
          stats = sessionParams[Stats].statsReceiver
        )
      )
    }

    override def newClient(dest: Name, label0: String): ServiceFactory[Request, Response] = {
      // We want to fail fast if the client's TLS configuration is inconsistent
      Mux.Client.validateTlsParamConsistency(dest, params)
      super.newClient(dest, label0)
    }

    protected def newPushTransporter(sa: SocketAddress): PushTransporter[ByteReader, Buf] = {
      // We use a custom Netty4PushTransporter to provide a handle to the
      // underlying Netty channel via MuxChannelHandle, giving us the ability to
      // add TLS support later in the lifecycle of the socket connection.
      new Netty4PushTransporter[ByteReader, Buf](
        transportInit = _ => (),
        protocolInit = PipelineInit,
        remoteAddress = sa,
        params = Mux.param.removeTlsIfOpportunisticClient(params)
      ) {
        override protected def initSession[T <: PushSession[ByteReader, Buf]](
          channel: Channel,
          protocolInit: (ChannelPipeline) => Unit,
          sessionBuilder: (PushChannelHandle[ByteReader, Buf]) => Future[T]
        ): Future[T] = {
          // With this builder we add support for opportunistic TLS via `MuxChannelHandle`
          // and the respective `Negotiation` types. Adding more proxy types will break this pathway.
          def wrappedBuilder(pushChannelHandle: PushChannelHandle[ByteReader, Buf]): Future[T] =
            sessionBuilder(new MuxChannelHandle(pushChannelHandle, channel, sessionParams))

          super.initSession(channel, protocolInit, wrappedBuilder)
        }
      }
    }

    protected def toService(
      session: MuxClientNegotiatingSession
    ): Future[Service[Request, Response]] =
      session.negotiate().flatMap(_.asService)

    protected def copy1(
      stack: Stack[ServiceFactory[Request, Response]],
      params: Stack.Params
    ): Client = copy(stack, params)
  }

  def client: Client = Client()

  def newService(dest: Name, label: String): Service[mux.Request, mux.Response] =
    client.newService(dest, label)

  def newClient(dest: Name, label: String): ServiceFactory[mux.Request, mux.Response] =
    client.newClient(dest, label)

  object Server {

    private[finagle] val stack: Stack[ServiceFactory[mux.Request, mux.Response]] =
      StackServer.newStack
      // We remove the trace init filter and don't replace it with anything because
      // the mux codec initializes tracing.
        .remove(TraceInitializerFilter.role)
        .prepend(ExportCompressionUsage.module)
        // Because tracing initialization happens in the mux codec, we know the service stack
        // is dispatched with proper tracing context, so the ordering of this filter isn't
        // relevant.
        .prepend(PayloadSizeFilter.serverModule(_.body.length, _.body.length))

    private[finagle] val tlsEnable: (Stack.Params, ChannelPipeline) => Unit = (params, pipeline) =>
      pipeline.addFirst("opportunisticSslInit", new Netty4ServerSslChannelInitializer(params))

    private[finagle] def params: Stack.Params = StackServer.defaultParams +
      ProtocolLibrary("mux") +
      param.TurnOnTlsFn(tlsEnable)

    type SessionF = (
      RefPushSession[ByteReader, Buf],
      Stack.Params,
      SharedNegotiationStats,
      MuxChannelHandle,
      Service[Request, Response]
    ) => PushSession[ByteReader, Buf]

    val defaultSessionFactory: SessionF = (
      ref: RefPushSession[ByteReader, Buf],
      params: Stack.Params,
      sharedStats: SharedNegotiationStats,
      handle: MuxChannelHandle,
      service: Service[Request, Response]
    ) => {
      val scopedStatsParams = params + Stats(params[Stats].statsReceiver.scope("mux"))
      MuxServerNegotiator.build(
        ref = ref,
        handle = handle,
        service = service,
        makeLocalHeaders = Mux.Server
          .headers(
            _: Headers,
            params[MaxFrameSize].size,
            params[OppTls].level.getOrElse(OpportunisticTls.Off),
            params[CompressionPreferences].compressionPreferences
          ),
        negotiate = (service, headers) =>
          new Negotiation.Server(scopedStatsParams, sharedStats, service)
            .negotiate(handle, headers),
        timer = params[Timer].timer
      )
      ref
    }

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
      tlsLevel: OpportunisticTls.Level,
      compressionPreferences: Compression.LocalPreferences
    ): Handshake.Headers = {
      val clientCompressionPreferences = Handshake
        .valueOf(CompressionNegotiation.ClientHeader.KeyBuf, clientHeaders)
        .map(CompressionNegotiation.ClientHeader.decode(_))
        .getOrElse(Compression.PeerCompressionOff)
      val compressionFormats = CompressionNegotiation.negotiate(
        compressionPreferences,
        clientCompressionPreferences
      )

      val withoutCompression = Seq(
        MuxFramer.Header.KeyBuf -> MuxFramer.Header.encodeFrameSize(maxFrameSize.inBytes.toInt),
        MuxOpportunisticTls.Header.KeyBuf -> tlsLevel.buf
      )

      if (compressionFormats.isDisabled) {
        withoutCompression
      } else {
        withoutCompression :+ (CompressionNegotiation.ServerHeader.KeyBuf ->
          CompressionNegotiation.ServerHeader.encode(compressionFormats))
      }
    }

    /**
     * Check the opportunistic TLS configuration to ensure it's in a consistent state
     */
    private def validateTlsParamConsistency(params: Stack.Params): Unit = {
      // We need to make sure
      if (OppTls.enabled(params) && params[ServerSsl].sslServerConfiguration.isEmpty) {
        val level = params[OppTls].level
        val label = params[Label].label
        throw new IllegalStateException(
          s"Server($label) desired opportunistic TLS ($level) but ServerSsl param is empty. Make" +
            s"sure your TLS configuration is properly loaded."
        )
      }
    }
  }

  final case class Server(
    stack: Stack[ServiceFactory[mux.Request, mux.Response]] = Mux.Server.stack,
    params: Stack.Params = Mux.Server.params,
    sessionFactory: Server.SessionF = Server.defaultSessionFactory)
      extends PushStackServer[mux.Request, mux.Response, Server]
      with OpportunisticTlsParams[Server]
      with WithCompressionPreferences[Server] {

    protected type PipelineReq = ByteReader
    protected type PipelineRep = Buf

    // Lazy so we don't accidentally instantiate stats before we have the appropriately
    // scoped stats receiver. See the Client shard stats for more detail.
    private[this] lazy val sessionStats = new SharedNegotiationStats(params[Stats].statsReceiver)

    protected def newListener(): PushListener[ByteReader, Buf] = {
      Mux.Server.validateTlsParamConsistency(params)
      new Netty4PushListener[ByteReader, Buf](
        pipelineInit = PipelineInit,
        params = Mux.param.removeTlsIfOpportunisticServer(params),
        setupMarshalling = identity
      ) {
        override protected def initializePushChannelHandle(
          ch: Channel,
          sessionFactory: SessionFactory
        ): Unit = {
          val proxyFactory: SessionFactory = { handle =>
            // We need to proxy via the MuxChannelHandle to get a vector
            // into the netty pipeline for handling installing the TLS
            // components of the pipeline after the negotiation.
            sessionFactory(new MuxChannelHandle(handle, ch, params))
          }
          super.initializePushChannelHandle(ch, proxyFactory)
        }
      }
    }

    protected def newSession(
      handle: PushChannelHandle[ByteReader, Buf],
      service: Service[Request, Response]
    ): RefPushSession[ByteReader, Buf] = {
      handle match {
        case h: MuxChannelHandle =>
          val ref = new RefPushSession[ByteReader, Buf](h, SentinelSession[ByteReader, Buf](h))
          sessionFactory(ref, params, sessionStats, h, service)
          ref

        case other =>
          throw new IllegalStateException(
            s"Expected to find a `MuxChannelHandle` but found ${other.getClass.getSimpleName}"
          )
      }
    }

    protected def copy1(
      stack: Stack[ServiceFactory[Request, Response]],
      params: Stack.Params
    ): Server = copy(stack, params)
  }

  def server: Server = Server()

  def serve(
    addr: SocketAddress,
    service: ServiceFactory[mux.Request, mux.Response]
  ): ListeningServer = server.serve(addr, service)
}
