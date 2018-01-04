package com.twitter.finagle.mux.exp.pushsession

import com.twitter.finagle.Mux.param.{MaxFrameSize, OppTls}
import com.twitter.finagle.exp.pushsession._
import com.twitter.finagle.mux.Handshake.Headers
import com.twitter.finagle.mux.{OpportunisticTlsParams, Request, Response}
import com.twitter.finagle.netty4.exp.pushsession.{Netty4PushListener, Netty4PushTransporter}
import com.twitter.finagle.server.StackServer
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

    private[this] def statsReceiver = params[param.Stats].statsReceiver.scope("mux")

    private[this] val buildParams = params + param.Stats(statsReceiver)

    protected type SessionT = MuxClientNegotiatingSession
    protected type In = ByteReader
    protected type Out = Buf

    protected def newSession(
      handle: PushChannelHandle[ByteReader, Buf]
    ): Future[MuxClientNegotiatingSession] = {
      val negotiator: Option[Headers] => MuxClientSession = Negotiation.Client(buildParams).negotiate(handle, _: Option[Headers])
      val headers = Mux.Client.headers(params[MaxFrameSize].size, params[OppTls].level)
      Future.value(
        new MuxClientNegotiatingSession(
          handle = handle,
          version = Mux.LatestVersion,
          negotiator = negotiator,
          headers = headers,
          name = params[param.Label].label))
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
        Mux.param.removeTlsIfOpportunisticClient(params) // we don't want to scope these metrics to mux
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
            sessionBuilder(new MuxChannelHandle(pushChannelHandle, channel, buildParams))

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
      Stack.Params,
      PushChannelHandle[ByteReader, Buf],
      Service[Request, Response]
    ) => PushSession[ByteReader, Buf]

    val defaultSessionFactory: SessionF = (
      params: Stack.Params,
      handle: PushChannelHandle[ByteReader, Buf],
      service: Service[Request, Response]
    ) => {
      MuxServerNegotiator(
        handle = handle,
        service = service,
        makeLocalHeaders = Mux.Server.headers(_: Headers, params[MaxFrameSize].size, params[OppTls].level),
        negotiate = (service, headers) => Negotiation.Server(params, service).negotiate(handle, headers),
        timer = params[param.Timer].timer
      )
    }
  }

  // TODO: support opp-TLS
  final case class Server(
    stack: Stack[ServiceFactory[mux.Request, mux.Response]] = Mux.Server().stack,
    params: Stack.Params = StackServer.defaultParams + param.ProtocolLibrary("mux"),
    sessionFactory: Server.SessionF = Server.defaultSessionFactory
  ) extends PushStackServer[mux.Request, mux.Response, Server] {

    protected type PipelineReq = ByteReader
    protected type PipelineRep = Buf

    private[this] val scopedStatsParams = params + param.Stats(
      params[param.Stats].statsReceiver.scope("mux"))

    protected def newListener(): PushListener[ByteReader, Buf] =
      new Netty4PushListener[ByteReader, Buf](
        MuxServerPipelineInit,
        params, // we don't want to use the scoped stats receiver
        identity
      )

    protected def newSession(
      handle: PushChannelHandle[ByteReader, Buf],
      service: Service[Request, Response]
    ): PushSession[ByteReader, Buf] =
      sessionFactory(scopedStatsParams, handle, service)

    protected def copy1(
      stack: Stack[ServiceFactory[Request, Response]],
      params: Stack.Params
    ): Server = copy(stack, params)
  }
}
