package com.twitter.finagle.kestrel.protocol

import com.twitter.finagle.memcached.protocol.text.client.{ClientFramer, ClientDecoder}
import com.twitter.finagle.memcached.protocol.text.server.{ServerDecoder, ServerFramer}
import com.twitter.finagle.memcached.protocol.text.{DecodingHandler, Encoder}
import com.twitter.finagle.netty3.codec.{FrameDecoderHandler, BufCodec}
import com.twitter.finagle._
import com.twitter.io.Buf
import org.jboss.netty.channel._
import scala.collection.immutable

private[finagle] object KestrelClientPipelineFactory extends ChannelPipelineFactory {
  def getPipeline(): ChannelPipeline = {
    val pipeline = Channels.pipeline()

    pipeline.addLast("bufCodec", new BufCodec)
    pipeline.addLast("framer", new FrameDecoderHandler(new ClientFramer))
    pipeline.addLast("decoder", new DecodingHandler(new ClientDecoder))
    pipeline.addLast("decoding2response", new DecodingToResponse)

    pipeline.addLast("encoder", new Encoder)
    pipeline.addLast("command2encoding", new CommandToEncoding)
    pipeline
  }
}

private[finagle] class Kestrel(failFast: Boolean) extends CodecFactory[Command, Response] {
  private[this] val storageCommands = immutable.Set[Buf](Buf.Utf8("set"))

  def this() = this(false)

  def server = Function.const {
    new Codec[Command, Response] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline(): ChannelPipeline = {
          val pipeline = Channels.pipeline()

          pipeline.addLast("bufCodec", new BufCodec)
          pipeline.addLast("framer", new FrameDecoderHandler(new ServerFramer(storageCommands)))
          pipeline.addLast("decoder", new DecodingHandler(new ServerDecoder(storageCommands)))
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
      val pipelineFactory = KestrelClientPipelineFactory

      // pass every request through a filter to create trace data
      override def prepareConnFactory(underlying: ServiceFactory[Command, Response], params: Stack.Params) =
        KestrelTracingFilter andThen underlying

      override def failFastOk: Boolean = failFast

      override def protocolLibraryName: String = Kestrel.this.protocolLibraryName
    }
  }

  override val protocolLibraryName: String = "kestrel"
}

object Kestrel {

  @deprecated("Use the com.twitter.finagle.Kestrel object to build a client", "2016-09-12")
  def apply(): CodecFactory[Command, Response] = apply(false)

  /**
   * NOTE: If you're using the codec to build a client connected to a single host, don't set this
   * to true. See CSL-288
   */
  @deprecated("Use the com.twitter.finagle.Kestrel object to build a client", "2016-09-12")
  def apply(failFast: Boolean): CodecFactory[Command, Response] = new Kestrel(failFast)

  @deprecated("Use the com.twitter.finagle.Kestrel object to build a client", "2016-09-12")
  def get() = apply()
}
