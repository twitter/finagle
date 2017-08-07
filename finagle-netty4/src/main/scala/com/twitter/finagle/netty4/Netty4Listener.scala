package com.twitter.finagle.netty4

import com.twitter.finagle._
import com.twitter.finagle.netty4.channel.ServerBridge
import com.twitter.finagle.netty4.transport.ChannelTransport
import com.twitter.finagle.server.Listener
import com.twitter.finagle.transport.Transport
import io.netty.channel._
import java.lang.{Integer => JInt}
import java.net.SocketAddress

private[finagle] object Netty4Listener {

  val TrafficClass: ChannelOption[JInt] = ChannelOption.newInstance("trafficClass")

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
private[finagle] case class Netty4Listener[In, Out](
  pipelineInit: ChannelPipeline => Unit,
  params: Stack.Params,
  transportFactory: Channel => Transport[Any, Any] = { ch: Channel =>
    new ChannelTransport(ch)
  },
  setupMarshalling: ChannelInitializer[Channel] => ChannelHandler = identity
)(implicit mIn: Manifest[In], mOut: Manifest[Out])
    extends Listener[In, Out] {

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
  def listen(addr: SocketAddress)(serveTransport: Transport[In, Out] => Unit): ListeningServer = {
    val bridge = new ServerBridge(transportFactory.andThen(Transport.cast[In, Out]), serveTransport)

    listeningServerBuilder.bindWithBridge(bridge, addr)
  }

  override def toString: String = "Netty4Listener"
}
