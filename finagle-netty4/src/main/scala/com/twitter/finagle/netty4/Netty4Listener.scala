package com.twitter.finagle.netty4

import com.twitter.finagle._
import com.twitter.finagle.netty4.channel.ServerBridge
import com.twitter.finagle.netty4.transport.ChannelTransport
import com.twitter.finagle.server.Listener
import com.twitter.finagle.transport.{Transport, TransportContext}
import io.netty.channel._
import java.lang.{Integer => JInt}
import java.net.SocketAddress

object Netty4Listener {

  private[finagle] val TrafficClass: ChannelOption[JInt] = ChannelOption.newInstance("trafficClass")

  /**
   * A [[com.twitter.finagle.Stack.Param]] used to configure the ability to
   * exert back pressure by only reading from the Channel when the [[Transport]] is
   * read.
   */
  private[finagle] case class BackPressure(enabled: Boolean) {
    def mk(): (BackPressure, Stack.Param[BackPressure]) = (this, BackPressure.param)
  }

  private[finagle] object BackPressure {
    implicit val param: Stack.Param[BackPressure] =
      Stack.Param(BackPressure(enabled = true))
  }

  /**
   * A [[com.twitter.finagle.Stack.Param]] used to configure the ability to limit the number
   * of connections to a listener.
   *
   * @note that this is implemented by eagerly hanging up once the limit has been reached
   *       and this can have detrimental effects for non-TLS protocols that don't handshake
   *       since the client may immediately send a request before the server has hung up.
   */
  case class MaxConnections(maxConnections: Long) {
    require(
      maxConnections > 0,
      s"MaxConnections must be a positive value. Observed: $maxConnections")
    def mk(): (MaxConnections, Stack.Param[MaxConnections]) = (this, MaxConnections.param)
  }

  object MaxConnections {
    implicit val param: Stack.Param[MaxConnections] = Stack.Param(MaxConnections(Long.MaxValue))
  }

  def apply[In, Out](
    pipelineInit: ChannelPipeline => Unit,
    params: Stack.Params,
    setupMarshalling: ChannelInitializer[Channel] => ChannelHandler
  )(
    implicit mIn: Manifest[In],
    mOut: Manifest[Out]
  ): Netty4Listener[In, Out, TransportContext] =
    Netty4Listener[In, Out, TransportContext](
      pipelineInit,
      params,
      setupMarshalling,
      new ChannelTransport(_)
    )

  def apply[In, Out](
    pipelineInit: ChannelPipeline => Unit,
    params: Stack.Params
  )(
    implicit mIn: Manifest[In],
    mOut: Manifest[Out]
  ): Netty4Listener[In, Out, TransportContext] =
    Netty4Listener[In, Out, TransportContext](
      pipelineInit,
      params,
      identity,
      new ChannelTransport(_)
    )
}

/**
 * Constructs a `Listener[In, Out]` given a ``pipelineInit`` function
 * responsible for framing a [[Transport]] stream. The [[Listener]] is configured
 * via the passed in [[com.twitter.finagle.Stack.Param Params]].
 *
 * @see [[com.twitter.finagle.server.Listener]]
 * @see [[com.twitter.finagle.transport.Transport]]
 * @see [[com.twitter.finagle.param]]
 */
case class Netty4Listener[In, Out, Ctx <: TransportContext](
  pipelineInit: ChannelPipeline => Unit,
  params: Stack.Params,
  setupMarshalling: ChannelInitializer[Channel] => ChannelHandler,
  transportFactory: Channel => Transport[Any, Any] {
    type Context <: Ctx
  }
)(
  implicit mIn: Manifest[In],
  mOut: Manifest[Out])
    extends Listener[In, Out, Ctx] {

  private[this] val listeningServerBuilder =
    new ListeningServerBuilder(pipelineInit, params, setupMarshalling)

  /**
   * Listen for connections and apply the `serveTransport` callback on
   * connected [[Transport transports]].
   *
   * @param addr socket address for listening.
   * @param serveTransport a call-back for newly created transports which in turn are
   *                       created for new connections.
   * @note the ``serveTransport`` implementation is responsible for calling
   *       [[Transport.close() close]] on  [[Transport transports]].
   */
  def listen(
    addr: SocketAddress
  )(
    serveTransport: Transport[In, Out] { type Context <: Ctx } => Unit
  ): ListeningServer = {
    val mkTrans = transportFactory
      .andThen(Transport.cast[In, Out])
      .asInstanceOf[Channel => Transport[In, Out] { type Context <: Ctx }]
    val bridge = new ServerBridge(mkTrans, serveTransport)

    listeningServerBuilder.bindWithBridge(bridge, addr)
  }

  override def toString: String = "Netty4Listener"
}
