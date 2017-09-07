package com.twitter.finagle.netty4.exp.pushsession

import com.twitter.finagle.Stack
import com.twitter.finagle.netty4.channel.RawNetty4ClientChannelInitializer
import com.twitter.finagle.netty4.ConnectionBuilder
import com.twitter.finagle.exp.pushsession.{PushChannelHandle, PushSession, PushTransporter}
import com.twitter.util.Future
import io.netty.channel.ChannelPipeline
import java.net.SocketAddress

object Netty4PushTransporter {

  private[this] def build[In, Out](
    protocolInit: ChannelPipeline => Unit,
    addr: SocketAddress,
    params: Stack.Params
  )(implicit mOut: Manifest[Out]): PushTransporter[In, Out] =
    new Netty4PushTransporter[In, Out](_ => (), protocolInit, addr, params)

  /**
   * `Transporter` constructor for protocols that need direct access to the netty pipeline
   * (e.g. finagle-http)
   *
   * @note this factory method makes no assumptions about reference counting
   *       of `ByteBuf` instances.
   */
  def raw[In, Out](
    protocolInit: ChannelPipeline => Unit,
    addr: SocketAddress,
    params: Stack.Params
  )(implicit mOut: Manifest[Out]): PushTransporter[In, Out] = {
    build[In, Out](protocolInit, addr, params)
  }
}

// Constructor exposed for testing
private final class Netty4PushTransporter[In, Out] private[pushsession] (
  transportInit: ChannelPipeline => Unit,
  protocolInit: ChannelPipeline => Unit,
  val remoteAddress: SocketAddress,
  params: Stack.Params
) extends PushTransporter[In, Out] {

  private[this] val builder = new ConnectionBuilder(
    new RawNetty4ClientChannelInitializer(transportInit, params),
    remoteAddress,
    params
  )

  def apply[T <: PushSession[In, Out]](
    sessionBuilder: (PushChannelHandle[In, Out]) => Future[T]
  ): Future[T] =
    builder.build { channel =>
      val (_, sessionF) =
        Netty4PushChannelHandle.install[In, Out, T](channel, protocolInit, sessionBuilder)
      sessionF
    }

  override def toString: String = "Netty4PushTransporter"
}
