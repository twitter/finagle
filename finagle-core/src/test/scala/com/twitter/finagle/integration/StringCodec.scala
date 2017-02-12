package com.twitter.finagle.integration

import com.twitter.finagle._
import java.nio.charset.StandardCharsets.UTF_8
import org.jboss.netty.channel.{ChannelPipelineFactory, Channels}
import org.jboss.netty.handler.codec.frame.{DelimiterBasedFrameDecoder, Delimiters}
import org.jboss.netty.handler.codec.string.{StringDecoder, StringEncoder}

object StringCodec extends StringCodec

class StringCodec extends CodecFactory[String, String] {
  def server = Function.const {
    new Codec[String, String] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("frameDecoder", new DelimiterBasedFrameDecoder(100, Delimiters.lineDelimiter: _*))
          pipeline.addLast("stringDecoder", new StringDecoder(UTF_8))
          pipeline.addLast("stringEncoder", new StringEncoder(UTF_8))
          pipeline
        }
      }
    }
  }

  def client = Function.const {
    new Codec[String, String] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("stringEncode", new StringEncoder(UTF_8))
          pipeline.addLast("stringDecode", new StringDecoder(UTF_8))
          pipeline
        }
      }

      override def prepareConnFactory(factory: ServiceFactory[String, String], ps: Stack.Params) =
        new AddNewlineFilter andThen factory
    }
  }

  class AddNewlineFilter extends SimpleFilter[String, String] {
    def apply(request: String, service: Service[String, String]) = service(request + "\n")
  }
}
