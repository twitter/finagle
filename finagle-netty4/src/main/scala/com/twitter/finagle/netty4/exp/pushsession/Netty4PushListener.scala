package com.twitter.finagle.netty4.exp.pushsession

import com.twitter.finagle.netty4.ListeningServerBuilder
import com.twitter.finagle.{ListeningServer, Stack}
import com.twitter.finagle.exp.pushsession.{PushChannelHandle, PushListener, PushSession}
import com.twitter.util.Future
import io.netty.channel.{Channel, ChannelHandler, ChannelInitializer, ChannelPipeline}
import java.net.SocketAddress

/**
 * Netty4 based PushListener implementation
 */
private[finagle] final class Netty4PushListener[In, Out](
  pipelineInit: ChannelPipeline => Unit,
  params: Stack.Params,
  setupMarshalling: ChannelInitializer[Channel] => ChannelHandler
) extends PushListener[In, Out] {

  private type SessionFactory = (PushChannelHandle[In, Out]) => Future[PushSession[In, Out]]

  private[this] val listeningServerBuilder =
    new ListeningServerBuilder(_ => (), params, setupMarshalling)

  def listen(addr: SocketAddress)(sessionFactory: SessionFactory): ListeningServer = {
    val initializer = new ChannelHandleInitializer(sessionFactory)
    listeningServerBuilder.bindWithBridge(initializer, addr)
  }

  private[this] class ChannelHandleInitializer(sessionFactory: SessionFactory)
      extends ChannelInitializer[Channel] {

    def initChannel(ch: Channel): Unit =
      Netty4PushChannelHandle.install[In, Out, PushSession[In, Out]](
        ch, pipelineInit, sessionFactory)
  }
}
