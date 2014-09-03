package com.twitter.finagle.server

import com.twitter.finagle.Stack
import com.twitter.finagle.Stack.Params
import com.twitter.finagle.dispatch.SerialServerDispatcher
import com.twitter.finagle.netty3.Netty3Listener
import com.twitter.io.Charsets
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.frame.{Delimiters, DelimiterBasedFrameDecoder}
import org.jboss.netty.handler.codec.string.{StringEncoder, StringDecoder}

private[finagle] object StringServerPipeline extends ChannelPipelineFactory {
  def getPipeline = {
    val pipeline = Channels.pipeline()
    pipeline.addLast("line", new DelimiterBasedFrameDecoder(100, Delimiters.lineDelimiter: _*))
    pipeline.addLast("stringDecoder", new StringDecoder(Charsets.Utf8))
    pipeline.addLast("stringEncoder", new StringEncoder(Charsets.Utf8))
    pipeline
  }
}

private[finagle] trait StringServer {
  val stringServer = new StackServer[String, String] {
    protected type In = String
    protected type Out = String

    protected val newListener: (Params) => Listener[In, Out] =
      Netty3Listener(StringServerPipeline, _)

    protected val newDispatcher: Stack.Params => Dispatcher =
      Function.const(new SerialServerDispatcher(_, _))
  }
}
