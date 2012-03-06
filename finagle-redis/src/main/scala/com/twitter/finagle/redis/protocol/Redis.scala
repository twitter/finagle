package com.twitter.finagle.redis

import protocol.{Command, CommandCodec, Reply, ReplyCodec}

import com.twitter.finagle.{Codec, CodecFactory, Service}
import com.twitter.finagle.tracing.ClientRequestTracingFilter
import com.twitter.naggati.{Codec => NaggatiCodec}
import com.twitter.util.Future
import org.jboss.netty.channel.{ChannelPipelineFactory, Channels}

object Redis {
  def apply() = new Redis
  def get() = apply()
}

class Redis extends CodecFactory[Command, Reply] {
  def server = Function.const {
    new Codec[Command, Reply] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline() = {
          val pipeline = Channels.pipeline()
          val commandCodec = new CommandCodec
          val replyCodec = new ReplyCodec

          pipeline.addLast("codec", new NaggatiCodec(commandCodec.decode, replyCodec.encode))

          pipeline
        }
      }
    }
  }

  def client = Function.const {
    new Codec[Command, Reply] {

      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline() = {
          val pipeline = Channels.pipeline()
          val commandCodec = new CommandCodec
          val replyCodec = new ReplyCodec

          pipeline.addLast("codec", new NaggatiCodec(replyCodec.decode, commandCodec.encode))

          pipeline
        }
      }

      override def prepareService(underlying: Service[Command, Reply]) = {
        Future.value((new RedisTracingFilter()) andThen underlying)
      }

    }
  }
}

private class RedisTracingFilter extends ClientRequestTracingFilter[Command, Reply] {
  val serviceName = "redis"
  def methodName(req: Command): String = req.getClass().getSimpleName()
}
