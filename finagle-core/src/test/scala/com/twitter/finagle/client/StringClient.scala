package com.twitter.finagle.client

import com.twitter.finagle.Stack
import com.twitter.finagle.dispatch.SerialClientDispatcher
import com.twitter.finagle.netty3.Netty3Transporter
import com.twitter.io.Charsets
import org.jboss.netty.channel.{MessageEvent, ChannelHandlerContext, SimpleChannelHandler, Channels, ChannelPipelineFactory}
import org.jboss.netty.handler.codec.string.{StringEncoder, StringDecoder}

private[client] object StringClientPipeline extends ChannelPipelineFactory {
  def getPipeline = {
    val pipeline = Channels.pipeline()
    pipeline.addLast("stringEncode", new StringEncoder(Charsets.Utf8))
    pipeline.addLast("stringDecode", new StringDecoder(Charsets.Utf8))
    pipeline.addLast("line", new DelimEncoder('\n'))
    pipeline
  }
}

private class DelimEncoder(delim: Char) extends SimpleChannelHandler {
  override def writeRequested(ctx: ChannelHandlerContext, evt: MessageEvent) = {
    val newMessage = evt.getMessage match {
      case m: String => m + delim
      case m => m
    }
    Channels.write(ctx, evt.getFuture, newMessage, evt.getRemoteAddress)
  }
}

private[finagle] trait StringClient {
  val stringClient = new StackClient[String, String] {
    protected type In = String
    protected type Out = String

    protected val newTransporter: Stack.Params => Transporter[String, String] =
      Netty3Transporter(StringClientPipeline, _)

    protected val newDispatcher: Stack.Params => Dispatcher =
      Function.const(new SerialClientDispatcher(_))
  }
}
