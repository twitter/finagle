package com.twitter.finagle.server

import com.twitter.finagle._
import com.twitter.finagle.param
import com.twitter.finagle.dispatch.SerialServerDispatcher
import com.twitter.finagle.netty4.Netty4Listener
import com.twitter.finagle.transport.Transport
import io.netty.channel.ChannelPipeline
import io.netty.handler.codec.string.{StringDecoder, StringEncoder}
import io.netty.handler.codec.{DelimiterBasedFrameDecoder, Delimiters}
import java.nio.charset.StandardCharsets

private[finagle] object StringServerPipeline extends (ChannelPipeline => Unit) {
  def apply(pipeline: ChannelPipeline): Unit = {
    pipeline.addLast("line", new DelimiterBasedFrameDecoder(100, Delimiters.lineDelimiter: _*))
    pipeline.addLast("stringDecoder", new StringDecoder(StandardCharsets.UTF_8))
    pipeline.addLast("stringEncoder", new StringEncoder(StandardCharsets.UTF_8))
  }
}

private[finagle] object StringServer {
  val protocolLibrary = "string"
}

trait StringServer {
  import StringServer._

  case class Server(
    stack: Stack[ServiceFactory[String, String]] = StackServer.newStack,
    params: Stack.Params = StackServer.defaultParams + param.ProtocolLibrary(protocolLibrary)
  ) extends StdStackServer[String, String, Server] {
    protected def copy1(
      stack: Stack[ServiceFactory[String, String]] = this.stack,
      params: Stack.Params = this.params
    ) = copy(stack, params)

    protected type In = String
    protected type Out = String

    protected def newListener() = Netty4Listener(StringServerPipeline, params)
    protected def newDispatcher(
      transport: Transport[In, Out],
      service: Service[String, String]
    ) = new SerialServerDispatcher(transport, service)
  }

  val stringServer = Server()
}
