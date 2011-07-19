package com.twitter.finagle.memcached.protocol.text

import client.DecodingToResponse
import org.jboss.netty.channel._
import com.twitter.finagle.memcached.protocol._
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import server.DecodingToCommand
import server.{Decoder => ServerDecoder}
import client.{Decoder => ClientDecoder}
import com.twitter.finagle.{SimpleFilter, Service, CodecFactory, Codec}
import com.twitter.finagle.tracing.{Annotation, Trace}
import com.twitter.util.Future

object Memcached {
  def apply() = new Memcached
  def get() = apply()
}

class Memcached extends CodecFactory[Command, Response] {
  private[this] val storageCommands = collection.Set[ChannelBuffer](
    "set", "add", "replace", "append", "prepend")

  def server = Function.const {
    new Codec[Command, Response] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline() = {
          val pipeline = Channels.pipeline()

  //        pipeline.addLast("exceptionHandler", new ExceptionHandler)

          pipeline.addLast("decoder", new ServerDecoder(storageCommands))
          pipeline.addLast("decoding2command", new DecodingToCommand)

          pipeline.addLast("encoder", new Encoder)
          pipeline.addLast("response2encoding", new ResponseToEncoding)
          pipeline
        }
      }
    }
  }

  def client = Function.const {
    new Codec[Command, Response] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline() = {
          val pipeline = Channels.pipeline()

          pipeline.addLast("decoder", new ClientDecoder)
          pipeline.addLast("decoding2response", new DecodingToResponse)

          pipeline.addLast("encoder", new Encoder)
          pipeline.addLast("command2encoding", new CommandToEncoding)
          pipeline
        }
      }

      // pass every request through a filter to create trace data
      override def prepareService(underlying: Service[Command, Response]) = {
        Future.value((new MemcachedTracingFilter()) andThen underlying)
      }
    }
  }
}

/**
 * Adds tracing information for each memcached request.
 * Including command name, when request was sent and when it was received.
 */
private class MemcachedTracingFilter extends SimpleFilter[Command, Response]
{
  def apply(
    request: Command,
    service: Service[Command, Response]
  ) = {
    Trace.recordRpcname("memcached", request.getClass().getSimpleName())
    Trace.record(Annotation.ClientSend())

    service(request) onSuccess { _ =>
      Trace.record(Annotation.ClientRecv())
    }
  }
}

