package com.twitter.finagle.netty4

import com.twitter.finagle.client.Transporter
import com.twitter.finagle.decoder.Framer
import com.twitter.finagle.netty4.channel._
import com.twitter.finagle.netty4.transport.ChannelTransport
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.finagle.Stack
import com.twitter.io.Buf
import com.twitter.util.Future
import io.netty.channel._
import java.net.SocketAddress

object Netty4Transporter {

  /**
   * A [[com.twitter.finagle.Stack.Param]] used to configure the ability to
   * exert backpressure by only reading from the Channel when the [[Transport]] is
   * read.
   */
  private[finagle] case class Backpressure(backpressure: Boolean) {
    def mk(): (Backpressure, Stack.Param[Backpressure]) = (this, Backpressure.param)
  }

  private[finagle] object Backpressure {
    implicit val param: Stack.Param[Backpressure] =
      Stack.Param(Backpressure(backpressure = true))
  }

  private[this] def build[In, Out, Ctx <: TransportContext](
    init: ChannelInitializer[Channel],
    addr: SocketAddress,
    params: Stack.Params,
    transportFactory: Channel => Transport[Any, Any] {
      type Context <: Ctx
    }
  )(
    implicit mOut: Manifest[Out]
  ): Transporter[In, Out, Ctx] = new Transporter[In, Out, Ctx] {

    private[this] val factory = new ConnectionBuilder(init, addr, params)

    def remoteAddress: SocketAddress = addr

    def apply(): Future[Transport[In, Out] { type Context <: Ctx }] = factory.build { ch =>
      Future {
        Transport
          .cast[In, Out](transportFactory(ch))
          .asInstanceOf[Transport[In, Out] { type Context <: Ctx }]
      }
    }

    override def toString: String = "Netty4Transporter"
  }

  /**
   * `Transporter` constructor for protocols that need direct access to the netty pipeline
   * (ie; finagle-http)
   *
   * @note this factory method makes no assumptions about reference counting
   *       of `ByteBuf` instances.
   */
  def raw[In, Out, Ctx <: TransportContext](
    pipelineInit: ChannelPipeline => Unit,
    addr: SocketAddress,
    params: Stack.Params,
    transportFactory: Channel => Transport[Any, Any] {
      type Context <: Ctx
    }
  )(
    implicit mOut: Manifest[Out]
  ): Transporter[In, Out, Ctx] = {
    val init = new RawNetty4ClientChannelInitializer(pipelineInit, params)

    build[In, Out, Ctx](init, addr, params, transportFactory)
  }

  /**
   * `Transporter` constructor for protocols that need direct access to the netty pipeline
   * (ie; finagle-http)
   *
   * @note this factory method makes no assumptions about reference counting
   *       of `ByteBuf` instances.
   */
  def raw[In, Out](
    pipelineInit: ChannelPipeline => Unit,
    addr: SocketAddress,
    params: Stack.Params
  )(
    implicit mOut: Manifest[Out]
  ): Transporter[In, Out, TransportContext] = {
    val init = new RawNetty4ClientChannelInitializer(pipelineInit, params)

    build[In, Out, TransportContext](init, addr, params, new ChannelTransport(_))
  }

  /**
   * `Transporter` constructor for protocols which are entirely implemented in
   * dispatchers (ie; finagle-mux, finagle-mysql) and expect c.t.io.Bufs
   *
   * @note this factory method will install the `DirectToHeapInboundHandler` which
   *       copies all direct `ByteBuf`s to heap allocated `ByteBuf`s and frees the
   *       direct buffer.
   */
  def framedBuf(
    framerFactory: Option[() => Framer],
    addr: SocketAddress,
    params: Stack.Params
  ): Transporter[Buf, Buf, TransportContext] = {
    val init = new Netty4ClientChannelInitializer(params, framerFactory)

    build[Buf, Buf, TransportContext](init, addr, params, new ChannelTransport(_))
  }
}
