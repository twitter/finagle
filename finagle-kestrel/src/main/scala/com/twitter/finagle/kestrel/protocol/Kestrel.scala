package com.twitter.finagle.kestrel.protocol

import org.jboss.netty.channel._
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.memcached.protocol.text.{Encoder, server, client}
import server.{Decoder => ServerDecoder}
import client.{Decoder => ClientDecoder}
import com.twitter.finagle.{ServiceFactory, Codec, CodecFactory}
import com.twitter.finagle.tracing.ClientRequestTracingFilter

class Kestrel(failFast: Boolean) extends CodecFactory[Command, Response] {
  private[this] val storageCommands = collection.Set[ChannelBuffer]("set")

  def this() = this(false)

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
      override def prepareConnFactory(underlying: ServiceFactory[Command, Response]) =
        new KestrelTracingFilter() andThen underlying

      override def failFastOk = failFast
    }
  }

  override val protocolLibraryName: String = "kestrel"
}

/**
 * Adds tracing information for each kestrel request.
 * Including command name, when request was sent and when it was received.
 */
private class KestrelTracingFilter extends ClientRequestTracingFilter[Command, Response] {
  val serviceName = "kestrel"
  def methodName(req: Command): String = req.name
}

object Kestrel {
  def apply(): CodecFactory[Command, Response] = apply(false)
  /**
   * NOTE: If you're using the codec to build a client connected to a single host, don't set this
   * to true. See CSL-288
   */
  def apply(failFast: Boolean): CodecFactory[Command, Response] = new Kestrel(failFast)
  def get() = apply()
}
