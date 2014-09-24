package com.twitter.finagle.client

import com.twitter.finagle.dispatch.SerialClientDispatcher
import com.twitter.finagle.netty3.Netty3Transporter
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{Stack, ServiceFactory}
import com.twitter.io.Charsets
import org.jboss.netty.channel.{
  ChannelHandlerContext, ChannelPipelineFactory, Channels, MessageEvent,
  SimpleChannelHandler}
import org.jboss.netty.handler.codec.string.{StringEncoder, StringDecoder}

private class DelimEncoder(delim: Char) extends SimpleChannelHandler {
  override def writeRequested(ctx: ChannelHandlerContext, evt: MessageEvent) = {
    val newMessage = evt.getMessage match {
      case m: String => m + delim
      case m => m
    }

    Channels.write(ctx, evt.getFuture, newMessage, evt.getRemoteAddress)
  }
}

private[client] object StringClientPipeline extends ChannelPipelineFactory {
  def getPipeline = {
    val pipeline = Channels.pipeline()
    pipeline.addLast("stringEncode", new StringEncoder(Charsets.Utf8))
    pipeline.addLast("stringDecode", new StringDecoder(Charsets.Utf8))
    pipeline.addLast("line", new DelimEncoder('\n'))
    pipeline
  }
}


private[finagle] trait StringClient {
  case class Client(stack: Stack[ServiceFactory[String, String]] = StackClient.newStack,
      params: Stack.Params = StackClient.defaultParams)
   extends StdStackClient[String, String, Client] {
    protected def copy1(
      stack: Stack[ServiceFactory[String, String]] = this.stack,
      params: Stack.Params = this.params
    ): Client = copy(stack, params)

    protected type In = String
    protected type Out = String

    protected def newTransporter(): Transporter[String, String] =
      Netty3Transporter(StringClientPipeline, params)

    protected def newDispatcher(transport: Transport[In, Out]) =
      new SerialClientDispatcher(transport)
  }

  val stringClient = Client()
}
