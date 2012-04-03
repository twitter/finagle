package com.twitter.finagle.memcached.protocol.text

import client.DecodingToResponse
import org.jboss.netty.channel._
import com.twitter.finagle.memcached.protocol._
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import server.DecodingToCommand
import server.{Decoder => ServerDecoder}
import client.{Decoder => ClientDecoder}
import com.twitter.finagle.{SimpleFilter, Service, ServiceFactory, CodecFactory, Codec}
import com.twitter.finagle.tracing._
import com.twitter.util.Future
import scala.collection.immutable
import org.jboss.netty.util.CharsetUtil.UTF_8
import com.twitter.finagle.memcached.util.ChannelBufferUtils._

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
      override def prepareConnFactory(underlying: ServiceFactory[Command, Response]) =
        new MemcachedTracingFilter() andThen underlying
    }
  }
}

/**
 * Adds tracing information for each memcached request.
 * Including command name, when request was sent and when it was received.
 */
private class MemcachedTracingFilter extends ClientRequestTracingFilter[Command, Response] {
  val serviceName = "memcached"
  def methodName(req: Command): String = req.getClass().getSimpleName()

  override def apply(command: Command, service: Service[Command, Response]) = Trace.unwind {
    Trace.recordRpcname(serviceName, methodName(command))
    Trace.record(Annotation.ClientSend())

    service(command) map { response =>
      Trace.record(Annotation.ClientRecv())
      response match {
        case Values(values) =>
          command match {
            case cmd: RetrievalCommand =>
              val keys = immutable.Set(cmd.keys map { _.toString(UTF_8) }: _*)
              val hits = values.map {
                case value =>
                  Trace.recordBinary(value.key.toString(UTF_8), "Hit")
                  value.key.toString(UTF_8)
              }
              val misses = keys -- hits
              misses foreach { k =>
                Trace.recordBinary(k.toString(UTF_8), "Miss")
              }
              case _ => response
          }
        case _  => response
      }
      response
    }
  }
}