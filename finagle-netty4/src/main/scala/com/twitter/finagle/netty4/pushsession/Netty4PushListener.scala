package com.twitter.finagle.netty4.pushsession

import com.twitter.finagle.netty4.ListeningServerBuilder
import com.twitter.finagle.{ListeningServer, Stack, param}
import com.twitter.finagle.pushsession.{PushChannelHandle, PushListener, PushSession}
import com.twitter.util.Future
import io.netty.channel.{Channel, ChannelHandler, ChannelInitializer, ChannelPipeline}
import java.net.SocketAddress

/**
 * Netty4 based PushListener implementation
 */
class Netty4PushListener[In, Out](
  pipelineInit: ChannelPipeline => Unit,
  params: Stack.Params,
  setupMarshalling: ChannelInitializer[Channel] => ChannelHandler)
    extends PushListener[In, Out] {

  final protected type SessionFactory = (PushChannelHandle[In, Out]) => Future[PushSession[In, Out]]

  private[this] val listeningServerBuilder =
    new ListeningServerBuilder(_ => (), params, setupMarshalling)

  final def listen(addr: SocketAddress)(sessionFactory: SessionFactory): ListeningServer = {
    val initializer = new ChannelHandleInitializer(sessionFactory)
    listeningServerBuilder.bindWithBridge(initializer, addr)
  }

  /**
   * Vector for hooking into the underlying Netty4 Channel.
   */
  protected def initializePushChannelHandle(ch: Channel, sessionFactory: SessionFactory): Unit = {
    val statsReceiver = params[param.Stats].statsReceiver
    Netty4PushChannelHandle.install[In, Out, PushSession[In, Out]](
      ch,
      pipelineInit,
      sessionFactory,
      statsReceiver
    )
  }

  private[this] class ChannelHandleInitializer(sessionFactory: SessionFactory)
      extends ChannelInitializer[Channel] {
    def initChannel(ch: Channel): Unit = initializePushChannelHandle(ch, sessionFactory)
  }

  override def toString: String = "Netty4PushListener"
}
