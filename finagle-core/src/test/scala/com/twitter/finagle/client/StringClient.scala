package com.twitter.finagle.client

import com.twitter.finagle.dispatch.SerialClientDispatcher
import com.twitter.finagle.netty3.Netty3Transporter
import com.twitter.finagle.Stack
import com.twitter.io.Charsets
import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}
import org.jboss.netty.handler.codec.frame.{Delimiters, DelimiterBasedFrameDecoder}
import org.jboss.netty.handler.codec.string.{StringEncoder, StringDecoder}

private[client] object StringClientPipeline extends ChannelPipelineFactory {
  def getPipeline = {
    val pipeline = Channels.pipeline()
    pipeline.addLast("stringEncode", new StringEncoder(Charsets.Utf8))
    pipeline.addLast("stringDecode", new StringDecoder(Charsets.Utf8))
    pipeline
  }
}

private[client] trait StringClient {
  val stringClient = new StackClient[String, String, String, String] {
    val newTransporter: Stack.Params => Transporter[String, String] =
      Netty3Transporter(StringClientPipeline, _)

    val newDispatcher: Stack.Params => Dispatcher =
      Function.const(new SerialClientDispatcher(_))
  }
}
