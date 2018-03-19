package com.twitter.finagle.mux.exp.pushsession

import com.twitter.finagle.Mux.param.{MaxFrameSize, OppTls}
import com.twitter.finagle.exp.pushsession._
import com.twitter.finagle.mux.Handshake.Headers
import com.twitter.finagle.mux.{OpportunisticTlsParams, Request, Response}
import com.twitter.finagle.netty4.exp.pushsession.{Netty4PushListener, Netty4PushTransporter}
import com.twitter.finagle._
import com.twitter.io.{Buf, ByteReader}
import com.twitter.logging.Logger
import com.twitter.util.Future
import io.netty.channel.{Channel, ChannelPipeline}
import java.net.{InetSocketAddress, SocketAddress}


/**
 * A push-based client for the mux protocol described in [[com.twitter.finagle.mux]].
 */
private[finagle] object MuxPush
    extends Client[mux.Request, mux.Response]
      with Server[mux.Request, mux.Response] {
  private val log = Logger.get

  def newService(dest: Name, label: String): Service[mux.Request, mux.Response] =
    client.newService(dest, label)

  def newClient(dest: Name, label: String): ServiceFactory[mux.Request, mux.Response] =
    client.newClient(dest, label)

  def serve(
    addr: SocketAddress,
    service: ServiceFactory[mux.Request, mux.Response]
  ): ListeningServer = server.serve(addr, service)

  def client: Client = Client()

  final case class Client(
    stack: Stack[ServiceFactory[mux.Request, mux.Response]] = Mux.Client().stack,
    params: Stack.Params = Mux.Client.params
  ) extends PushStackClient[mux.Request, mux.Response, Client]
      with param.WithDefaultLoadBalancer[Client]
      with OpportunisticTlsParams[Client] {

    private[this] val scopedStatsParams = params + param.Stats(
      params[param.Stats].statsReceiver.scope("mux"))

    protected type SessionT = MuxClientNegotiatingSession
    protected type In = ByteReader
    protected type Out = Buf

    protected def newSession(
      handle: PushChannelHandle[ByteReader, Buf]
    ): Future[MuxClientNegotiatingSession] = {
      val negotiator: Option[Headers] => MuxClientSession =
        new Negotiation.Client(scopedStatsParams).negotiate(handle, _: Option[Headers])
      val headers = Mux.Client.headers(params[MaxFrameSize].size, params[OppTls].level)
      Future.value(
        new MuxClientNegotiatingSession(
          handle = handle,
          version = Mux.LatestVersion,
          negotiator = negotiator,
          headers = headers,
          name = params[param.Label].label,
          stats = scopedStatsParams[param.Stats].statsReceiver))
    }

    override def newClient(
      dest: Name,
      label0: String
    ): ServiceFactory[Request, Response] = {
      // We want to fail fast if the client's TLS configuration is inconsistent
      Mux.Client.validateTlsParamConsistency(params)
      super.newClient(dest, label0)
    }

    protected def newPushTransporter(
      inetSocketAddress: InetSocketAddress
    ): PushTransporter[ByteReader, Buf] = {

      // We use a custom Netty4PushTransporter to provide a handle to the
      // underlying Netty channel via MuxChannelHandle, giving us the ability to
      // add TLS support later in the lifecycle of the socket connection.
      new Netty4PushTransporter[ByteReader, Buf](
        _ => (),
        MuxServerPipelineInit,
        inetSocketAddress,
        // we don't want to scope these metrics to mux, so we use `params`
        Mux.param.removeTlsIfOpportunisticClient(params)
      ) {
        override protected def initSession[T <: PushSession[ByteReader, Buf]](
          channel: Channel,
          protocolInit: (ChannelPipeline) => Unit,
          sessionBuilder: (PushChannelHandle[ByteReader, Buf]) => Future[T]
        ): Future[T] = {
          // With this builder we add support for opportunistic TLS which is
          // required in the `negotiateClientSession` method above. Adding
          // more proxy types will break this pathway.
          def wrappedBuilder(pushChannelHandle: PushChannelHandle[ByteReader, Buf]): Future[T] =
            sessionBuilder(new MuxChannelHandle(pushChannelHandle, channel, scopedStatsParams))

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

  def server: Server = Server()

  object Server {
    type SessionF = (
      RefPushSession[ByteReader, Buf],
      Stack.Params,
      MuxChannelHandle,
      Service[Request, Response]
    ) => PushSession[ByteReader, Buf]

    val defaultSessionFactory: SessionF = (
      ref: RefPushSession[ByteReader, Buf],
      params: Stack.Params,
      handle: MuxChannelHandle,
      service: Service[Request, Response]
    ) => {
      val scopedStatsParams = params + param.Stats(
        params[param.Stats].statsReceiver.scope("mux"))
      MuxServerNegotiator.build(
        ref = ref,
        handle = handle,
        service = service,
        makeLocalHeaders = Mux.Server
          .headers(_: Headers, params[MaxFrameSize].size, params[OppTls].level),
        negotiate = (service, headers) =>
          new Negotiation.Server(scopedStatsParams, service).negotiate(handle, headers),
        timer = params[param.Timer].timer
      )
      ref
    }
  }

  final case class Server(
    stack: Stack[ServiceFactory[mux.Request, mux.Response]] = Mux.server.stack,
    params: Stack.Params = Mux.server.params,
    sessionFactory: Server.SessionF = Server.defaultSessionFactory
  ) extends PushStackServer[mux.Request, mux.Response, Server]
    with OpportunisticTlsParams[Server] {

    protected type PipelineReq = ByteReader
    protected type PipelineRep = Buf

    protected def newListener(): PushListener[ByteReader, Buf] = {
      Mux.Server.validateTlsParamConsistency(params)
      new Netty4PushListener[ByteReader, Buf](
        MuxServerPipelineInit,
        Mux.param.removeTlsIfOpportunisticServer(params), // we don't want to scope these metrics to mux
        identity
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
    ): RefPushSession[ByteReader, Buf] = handle match {
      case h: MuxChannelHandle =>
        val ref = new RefPushSession[ByteReader, Buf](h, SentinelSession[ByteReader, Buf](h))
        sessionFactory(ref, params, h, service)
        ref

      case other =>
        throw new IllegalStateException(
          s"Expected to find a `MuxChannelHandle` but found ${other.getClass.getSimpleName}")
    }


    protected def copy1(
      stack: Stack[ServiceFactory[Request, Response]],
      params: Stack.Params
    ): Server = copy(stack, params)
  }
}
