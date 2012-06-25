package com.twitter.finagle.memcached.protocol.text

import client.DecodingToResponse
import client.{Decoder => ClientDecoder}
import server.DecodingToCommand
import server.{Decoder => ServerDecoder}
import com.twitter.finagle._
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.tracing._
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.util.Future
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel._
import org.jboss.netty.util.CharsetUtil.UTF_8
import scala.collection.immutable

object Memcached {
  def apply(stats: StatsReceiver = NullStatsReceiver) = new Memcached(stats)
  def get() = apply()
}

class Memcached(stats: StatsReceiver) extends CodecFactory[Command, Response] {

  def this() = this(NullStatsReceiver)

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
        new MemcachedTracingFilter() andThen new MemcachedLoggingFilter(stats) andThen underlying
    }
  }
}

/**
 * Adds tracing information for each memcached request.
 * Including command name, when request was sent and when it was received.
 */
private class MemcachedTracingFilter extends SimpleFilter[Command, Response] {
  def apply(command: Command, service: Service[Command, Response]) = Trace.unwind {
    Trace.recordRpcname("memcached", command.name)
    Trace.record(Annotation.ClientSend())

    service(command) map { response =>
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
      Trace.record(Annotation.ClientRecv())
      response
    }
  }
}

private class MemcachedLoggingFilter(stats: StatsReceiver)
  extends SimpleFilter[Command, Response] {

  private[this] val serviceName = "memcached"

  private[this] val error = stats.scope("error")
  private[this] val succ  = stats.scope("success")

  override def apply(command: Command, service: Service[Command, Response]) = {
    service(command) map { response =>
      response match {
        case NotFound()
          | Stored()
          | NotStored()
          | Exists()
          | Deleted()
          | NoOp()
          | Info(_, _)
          | InfoLines(_)
          | Values(_)
          | Number(_)      => succ.counter(command.name).incr()
        case Error(_)      => error.counter(command.name).incr()
        case _             => error.counter(command.name).incr()
      }
      response
    }
  }
}