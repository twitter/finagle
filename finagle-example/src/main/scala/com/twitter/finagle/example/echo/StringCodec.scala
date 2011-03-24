package com.twitter.finagle.example.echo

import com.twitter.finagle.{Codec, ClientCodec, ServerCodec}
import org.jboss.netty.handler.codec.string.{StringEncoder, StringDecoder}
import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}
import org.jboss.netty.handler.codec.frame.{Delimiters, DelimiterBasedFrameDecoder}
import org.jboss.netty.util.CharsetUtil

/**
 * A really simple demonstration of a custom Codec. This Codec is a newline (\n)
 * delimited line-based protocol. Here we re-use existing encoders/decoders as
 * provided by Netty.
 */
object StringCodec extends StringCodec

class StringCodec extends Codec[String, String] {
  override def serverCodec = new ServerCodec[String, String] {
    def pipelineFactory = new ChannelPipelineFactory {
      def getPipeline = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("line",
          new DelimiterBasedFrameDecoder(100, Delimiters.lineDelimiter: _*))
        pipeline.addLast("stringDecoder", new StringDecoder(CharsetUtil.UTF_8))
        pipeline.addLast("stringEncoder", new StringEncoder(CharsetUtil.UTF_8))
        pipeline
      }
    }
  }

  override def clientCodec = new ClientCodec[String, String] {
    def pipelineFactory = new ChannelPipelineFactory {
      def getPipeline = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("stringEncode", new StringEncoder(CharsetUtil.UTF_8))
        pipeline.addLast("stringDecode", new StringDecoder(CharsetUtil.UTF_8))
        pipeline
      }
    }
  }
}
