package com.twitter.finagle.mux.exp.pushsession


import com.twitter.finagle.Mux.param.MaxFrameSize
import com.twitter.finagle.Stack.Params
import com.twitter.finagle.client.StackClient
import com.twitter.finagle.exp.pushsession.{PushChannelHandle, PushStackClient, PushTransporter}
import com.twitter.finagle.liveness.FailureDetector
import com.twitter.finagle.mux.Handshake.Headers
import com.twitter.finagle.mux.transport.MuxFramer
import com.twitter.finagle.mux.{Handshake, Request, Response}
import com.twitter.finagle.netty4.exp.pushsession.{ Netty4PushTransporter}
import com.twitter.finagle.param.{ProtocolLibrary, WithDefaultLoadBalancer}
import com.twitter.finagle.{Client, Mux, Name, Service, ServiceFactory, Stack, mux, param}
import com.twitter.io.{Buf, ByteReader}
import com.twitter.util.Future
import java.net.InetSocketAddress


/**
 * A push-based client for the mux protocol described in [[com.twitter.finagle.mux]].
 */
private[finagle] object MuxPush
    extends Client[mux.Request, mux.Response] {

  def newService(dest: Name, label: String): Service[mux.Request, mux.Response] =
    client.newService(dest, label)

  def newClient(dest: Name, label: String): ServiceFactory[mux.Request, mux.Response] =
    client.newClient(dest, label)

  private[pushsession] def negotiateClientSession(
    handle: PushChannelHandle[ByteReader, Buf],
    params: Params,
    headers: Option[Headers] // unused, for now
  ): MuxClientSession = {
    val writeManager = {
      val fragmentSize = headers
        .flatMap(Handshake.valueOf(MuxFramer.Header.KeyBuf, _))
        .map(MuxFramer.Header.decodeFrameSize(_))
        .getOrElse(Int.MaxValue)
      new FragmentingMessageWriter(handle, fragmentSize)
    }

    val FailureDetector.Param(detectorConfig) = params[FailureDetector.Param]
    val name = params[param.Label].label
    val timer = params[param.Timer].timer
    val statsReceiver = params[param.Stats].statsReceiver

    new MuxClientSession(
      handle,
      new FragmentDecoder,
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
    params: Stack.Params = StackClient.defaultParams + ProtocolLibrary("mux")
  ) extends PushStackClient[mux.Request, mux.Response, Client]
      with WithDefaultLoadBalancer[Client] {

    private[this] def statsReceiver = params[param.Stats].statsReceiver.scope("mux")

    private[this] val buildParams = params + param.Stats(statsReceiver)

    protected type SessionT = MuxClientNegotiatingSession
    protected type In = ByteReader
    protected type Out = Buf

    protected def newSession(
      handle: PushChannelHandle[ByteReader, Buf]
    ): Future[MuxClientNegotiatingSession] = {
      val negotiator = negotiateClientSession(handle, buildParams, _: Option[Headers])
      val maxFrameSize = params[MaxFrameSize].size
      val name = params[param.Label].label
      Future.value(
        new MuxClientNegotiatingSession(handle, Mux.LatestVersion, negotiator, maxFrameSize, name))
    }

    protected def newPushTransporter(
      inetSocketAddress: InetSocketAddress
    ): PushTransporter[ByteReader, Buf] = {
      Netty4PushTransporter.raw[ByteReader, Buf](
        MuxServerPipelineInit,
        inetSocketAddress,
        buildParams
      )
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
