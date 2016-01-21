package com.twitter.finagle.netty4

import com.twitter.finagle.Stack
import com.twitter.finagle.client.{LatencyCompensation, Transporter}
import com.twitter.finagle.codec.{FrameDecoder, FrameEncoder}
import com.twitter.finagle.netty4.transport.ChannelTransport
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Throw, Future, Promise}
import io.netty.bootstrap.Bootstrap
import io.netty.buffer.UnpooledByteBufAllocator
import io.netty.channel._
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.util.concurrent.GenericFutureListener
import java.lang.{Boolean => JBool, Integer => JInt}
import java.net.SocketAddress

private[netty4] object Netty4Transporter {
  def apply[In, Out](
    params: Stack.Params,
    enc: Option[FrameEncoder[In]],
    decoderFactory: Option[() => FrameDecoder[Out]],
    transportFactory: Channel => Transport[In, Out] = new ChannelTransport[In, Out](_)
  ): Transporter[In, Out] = new Transporter[In, Out] {
    def apply(addr: SocketAddress): Future[Transport[In, Out]] = {

      // transportP is passed to ConnectionHandler in
      // Netty4ClientChannelInitializer and is satisfied when a
      // connected Transport is created. Its interrupt handler
      // should not be overridden.
      val transportP = new Promise[Transport[In, Out]]

      val channelInit =
        new Netty4ClientChannelInitializer[In, Out](
          transportP,
          params,
          enc,
          decoderFactory
        )

      val Transport.Options(noDelay, reuseAddr) = params[Transport.Options]
      val LatencyCompensation.Compensation(compensation) = params[LatencyCompensation.Compensation]
      val Transporter.ConnectTimeout(connectTimeout) = params[Transporter.ConnectTimeout]
      val Transport.BufferSizes(sendBufSize, recvBufSize) = params[Transport.BufferSizes]

      // max connect timeout is ~24.8 days
      val compensatedConnectTimeoutMs =
        (compensation + connectTimeout).inMillis.min(Int.MaxValue)

      val bootstrap =
        new Bootstrap()
          .group(WorkerPool)
          .channel(classOf[NioSocketChannel])
          // todo: investigate pooled allocator CSL-2089
          .option(ChannelOption.ALLOCATOR, UnpooledByteBufAllocator.DEFAULT)
          .option[JBool](ChannelOption.TCP_NODELAY, noDelay)
          .option[JBool](ChannelOption.SO_REUSEADDR, reuseAddr)
          .option[JBool](ChannelOption.AUTO_READ, false) // backpressure! no reads on transport => no reads on the socket
          .option[JInt](ChannelOption.CONNECT_TIMEOUT_MILLIS, compensatedConnectTimeoutMs.toInt)
          .handler(channelInit)

      val Transport.Liveness(_, _, keepAlive) = params[Transport.Liveness]
      keepAlive.foreach(bootstrap.option[JBool](ChannelOption.SO_KEEPALIVE, _))
      sendBufSize.foreach(bootstrap.option[JInt](ChannelOption.SO_SNDBUF, _))
      recvBufSize.foreach(bootstrap.option[JInt](ChannelOption.SO_RCVBUF, _))

      val nettyConnectF = bootstrap.connect(addr)

      // try to cancel the connect attempt if the transporter's promise is interrupted.
      transportP.setInterruptHandler { case _ => nettyConnectF.cancel(true /* mayInterruptIfRunning */) }

      nettyConnectF.addListener(new GenericFutureListener[ChannelPromise] {
        def operationComplete(channelP: ChannelPromise): Unit =
          if (channelP.cause != null) transportP.updateIfEmpty(Throw(channelP.cause))
      })

      transportP
    }
  }
}