package com.twitter.finagle.thrift

import org.apache.thrift.protocol.{TBinaryProtocol, TMessage, TMessageType}
import org.apache.thrift.transport.{TMemoryBuffer, TMemoryInputTransport}

import org.jboss.netty.channel.{
  SimpleChannelHandler, Channel, ChannelEvent, ChannelHandlerContext,
  SimpleChannelDownstreamHandler, MessageEvent, Channels,
  ChannelPipelineFactory}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder
import org.jboss.netty.channel.SimpleChannelUpstreamHandler

import com.twitter.finagle.Codec

class ThriftServerChannelBufferEncoder extends SimpleChannelDownstreamHandler {
  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) = {
    e.getMessage match {
      // An empty array indicates a oneway reply.
      case array: Array[Byte] if (!array.isEmpty) =>
        val buffer = ChannelBuffers.wrappedBuffer(array)
        Channels.write(ctx, e.getFuture, buffer)
      case array: Array[Byte] => ()
      case _ => throw new IllegalArgumentException("no byte array")
    }
  }
}

class ThriftServerTracer extends SimpleChannelUpstreamHandler {
  // Only if we've received an upgrade message, otherwise remove
  // ourselves from the pipeline.

  private[this] var isUpgraded = false

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    require(e.getMessage.isInstanceOf[Array[Byte]])
    val bytes = e.getMessage.asInstanceOf[Array[Byte]]
    if (isUpgraded) {
      println("server is upgraded.. setting txids, etc.")
      
      // Future?
      Channels.fireMessageReceived(ctx, Tracing.decode(bytes))

    } else {
      val protocolFactory = new TBinaryProtocol.Factory()
      val memoryTransport = new TMemoryInputTransport(bytes)
      val iprot = protocolFactory.getProtocol(memoryTransport)

      val msg = iprot.readMessageBegin()
      if (msg.`type` == TMessageType.CALL && msg.name == "__can__twitter__trace__") {
        println("UPGRADING")

        // upgrade & reply.
        isUpgraded = true

        val memoryBuffer = new TMemoryBuffer(512)
        val protocolFactory = new TBinaryProtocol.Factory()
        val oprot = protocolFactory.getProtocol(memoryBuffer)
        oprot.writeMessageBegin(
          new TMessage("__can__twitter__trace__", TMessageType.REPLY, msg.seqid))
        oprot.writeMessageEnd()

        val reply = java.util.Arrays.copyOfRange(memoryBuffer.getArray(), 0, memoryBuffer.length())
        Channels.write(ctx, Channels.future(ctx.getChannel), reply)
      } else {
        super.messageReceived(ctx, e)
      }
    }
  }
}

object ThriftServerFramedCodec {
  def apply() = new ThriftServerFramedCodec
}

class ThriftServerFramedCodec extends Codec[Array[Byte], Array[Byte]] {
  val clientPipelineFactory =
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("thriftFrameCodec", new ThriftFrameCodec)
        pipeline.addLast("byteEncoder", new ThriftServerChannelBufferEncoder)
        pipeline.addLast("byteDecoder", new ThriftChannelBufferDecoder)
        pipeline.addLast("tracer", new ThriftServerTracer)
        pipeline
      }
    }

  val serverPipelineFactory = clientPipelineFactory
}
