package com.twitter.finagle.kestrel.protocol

import com.twitter.finagle.memcached.protocol.text.client.{Decoder => ClientDecoder}
import com.twitter.finagle.memcached.protocol.text.server.{Decoder => ServerDecoder}
import com.twitter.finagle.memcached.protocol.text.{BufToChannelBuf, Encoder}
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.{Codec, CodecFactory, KestrelTracingFilter, ServiceFactory, Stack}
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel._

private[finagle] object KestrelClientPipelineFactory extends ChannelPipelineFactory {
  def getPipeline(): ChannelPipeline = {
    val pipeline = Channels.pipeline()

    pipeline.addLast("decoder", new ClientDecoder)
    pipeline.addLast("decoding2response", new DecodingToResponse)

    pipeline.addLast("buf2channelBuf", new BufToChannelBuf)
    pipeline.addLast("encoder", new Encoder)
    pipeline.addLast("command2encoding", new CommandToEncoding)
    pipeline
  }
}

class Kestrel(failFast: Boolean) extends CodecFactory[Command, Response] {
  private[this] val storageCommands = collection.Set[ChannelBuffer]("set")

  def this() = this(false)

  def server = Function.const {
    new Codec[Command, Response] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline(): ChannelPipeline = {
          val pipeline = Channels.pipeline()

          pipeline.addLast("decoder", new ServerDecoder(storageCommands))
          pipeline.addLast("decoding2command", new DecodingToCommand)

          pipeline.addLast("buf2channelBuf", new BufToChannelBuf)
          pipeline.addLast("encoder", new Encoder)
          pipeline.addLast("response2encoding", new ResponseToEncoding)
          pipeline
        }
      }
    }
  }

  def client = Function.const {
    new Codec[Command, Response] {
      val pipelineFactory = KestrelClientPipelineFactory

      // pass every request through a filter to create trace data
      override def prepareConnFactory(underlying: ServiceFactory[Command, Response], params: Stack.Params) =
        KestrelTracingFilter andThen underlying

      override def failFastOk = failFast
    }
  }

  override val protocolLibraryName: String = "kestrel"
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
