package com.twitter.finagle.mux.exp.pushsession


import com.twitter.finagle.Mux.param.{MaxFrameSize, OppTls}
import com.twitter.finagle.Stack.Params
import com.twitter.finagle.exp.pushsession.{PushChannelHandle, PushSession, PushStackClient, PushTransporter}
import com.twitter.finagle.liveness.FailureDetector
import com.twitter.finagle.mux.Handshake.Headers
import com.twitter.finagle.mux.transport.{IncompatibleNegotiationException, MuxFramer, OpportunisticTls}
import com.twitter.finagle.mux.{Handshake, OpportunisticTlsParams, Request, Response}
import com.twitter.finagle.netty4.exp.pushsession.Netty4PushTransporter
import com.twitter.finagle.param.WithDefaultLoadBalancer
import com.twitter.finagle.{Client, Mux, Name, Service, ServiceFactory, Stack, mux, param}
import com.twitter.io.{Buf, ByteReader}
import com.twitter.logging.{Level, Logger}
import com.twitter.util.Future
import io.netty.channel.{Channel, ChannelPipeline}
import java.net.InetSocketAddress


/**
 * A push-based client for the mux protocol described in [[com.twitter.finagle.mux]].
 */
private[finagle] object MuxPush
    extends Client[mux.Request, mux.Response] {
  private val log = Logger.get

  def newService(dest: Name, label: String): Service[mux.Request, mux.Response] =
    client.newService(dest, label)

  def newClient(dest: Name, label: String): ServiceFactory[mux.Request, mux.Response] =
    client.newClient(dest, label)

  private[pushsession] def negotiateClientSession(
    handle: PushChannelHandle[ByteReader, Buf],
    params: Params,
    peerHeaders: Option[Headers]
  ): MuxClientSession = {

    def turnOnTls(): Unit = handle match {
      case h: MuxChannelHandle => h.turnOnTls()
      case other =>
        // Should never happen when building a true client
        throw new IllegalStateException(
          s"Expected to find a MuxChannelHandle, instead found $other. Couldn't turn on TLS")
    }

    val statsReceiver = params[param.Stats].statsReceiver
    val localEncryptLevel = params[OppTls].level.getOrElse(OpportunisticTls.Off)

    val remoteEncryptLevel = peerHeaders.flatMap(Handshake.valueOf(OpportunisticTls.Header.KeyBuf, _)) match {
      case Some(buf) => OpportunisticTls.Header.decodeLevel(buf)
      case None =>
        log.debug("Peer either didn't negotiate or didn't send an Opportunistic Tls preference: " +
          "defaulting to remote encryption level of Off")
        OpportunisticTls.Off
    }

    try {
      val useTls = OpportunisticTls.negotiate(localEncryptLevel, remoteEncryptLevel)
      if (log.isLoggable(Level.DEBUG)) {
        log.debug(s"Successfully negotiated TLS with remote peer. Using TLS: $useTls " +
          s"local level: $localEncryptLevel, remote level: $remoteEncryptLevel")
      }
      if (useTls) {
        statsReceiver.counter("tls", "upgrade", "success").incr()
        turnOnTls()
      }
    } catch {
      case exn: IncompatibleNegotiationException =>
        log.fatal(
          exn,
          s"The local peer wanted $localEncryptLevel and the remote peer wanted" +
            s" $remoteEncryptLevel which are incompatible."
        )
        throw exn
    }

    val framingStats = statsReceiver.scope("framer")

    val writeManager = {
      val fragmentSize = peerHeaders
        .flatMap(Handshake.valueOf(MuxFramer.Header.KeyBuf, _))
        .map(MuxFramer.Header.decodeFrameSize(_))
        .getOrElse(Int.MaxValue)
      new FragmentingMessageWriter(handle, fragmentSize, framingStats)
    }

    val FailureDetector.Param(detectorConfig) = params[FailureDetector.Param]
    val name = params[param.Label].label
    val timer = params[param.Timer].timer

    new MuxClientSession(
      handle,
      new FragmentDecoder(framingStats),
      writeManager,
      detectorConfig,
      name,
      statsReceiver,
      timer
    )
  }

  def client: Client = Client()

  case class Client(
    stack: Stack[ServiceFactory[mux.Request, mux.Response]] = Mux.Client().stack,
    params: Stack.Params = Mux.Client.params
  ) extends PushStackClient[mux.Request, mux.Response, Client]
      with WithDefaultLoadBalancer[Client]
      with OpportunisticTlsParams[Client] {

    private[this] def statsReceiver = params[param.Stats].statsReceiver.scope("mux")

    private[this] val buildParams = params + param.Stats(statsReceiver)

    protected type SessionT = MuxClientNegotiatingSession
    protected type In = ByteReader
    protected type Out = Buf

    protected def newSession(
      handle: PushChannelHandle[ByteReader, Buf]
    ): Future[MuxClientNegotiatingSession] = {
      val negotiator = negotiateClientSession(handle, buildParams, _: Option[Headers])
      val headers = Mux.Client.headers(params[MaxFrameSize].size, params[OppTls].level)
      val name = params[param.Label].label
      Future.value(
        new MuxClientNegotiatingSession(
          handle = handle,
          version = Mux.LatestVersion,
          negotiator = negotiator,
          headers = headers,
          name = name))
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
        Mux.param.removeTlsIfOpportunisticClient(buildParams)
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
}
