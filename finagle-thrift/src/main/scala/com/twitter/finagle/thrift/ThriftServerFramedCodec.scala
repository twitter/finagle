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

import com.twitter.util.Future
import com.twitter.finagle._
import com.twitter.finagle.util.TracingHeader
import com.twitter.finagle.tracing.TraceContext

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

object ThriftServerFramedCodec {
  def apply() = new ThriftServerFramedCodec
}

// TODO: use LeftFoldChannelHandler

class ThriftServerTracingFilter
  extends SimpleFilter[Array[Byte], Array[Byte]]
{
  // Concurrency is not an issue here since we have an instance per
  // channel, and receive only one request at a time (thrift does no
  // pipelining). Furthermore, finagle will guarantee this by
  // serializing requests.
  private[this] var isUpgraded = false
  private[this] val protocolFactory = new TBinaryProtocol.Factory()

  def apply(request: Array[Byte], service: Service[Array[Byte], Array[Byte]]) = {
    // What to do on exceptions here?
    if (isUpgraded) {
      val header = new TracedRequest
      val request_ = InputBuffer.peelMessage(request, header)

      // TODO: Check isset?
      TraceContext().parentSpanID = Some(header.getParent_span_id)

      // has a problem with dups-- but we filter them out.
      
      service(request_) map { response =>
        // Wrap some trace data.
        val header = new TracedResponse
        val headerBytes = OutputBuffer.messageToArray(header)
        headerBytes ++ response
      }
    } else {
      val buffer = new InputBuffer(request)
      val msg = buffer().readMessageBegin()

      // TODO: only try once?
      if (msg.`type` == TMessageType.CALL &&
          msg.name == ThriftTracing.CanTraceMethodName) {
        // upgrade & reply.
        isUpgraded = true

        val buffer = new OutputBuffer
        buffer().writeMessageBegin(
          new TMessage(ThriftTracing.CanTraceMethodName, TMessageType.REPLY, msg.seqid))
        buffer().writeMessageEnd()

        // TODO: parse out options?
        Future.value(buffer.toArray)
      } else {
        service(request)
      }
    }
  }
}

class ThriftServerFramedCodec extends Codec[Array[Byte], Array[Byte]] {
  val clientPipelineFactory =
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("thriftFrameCodec", new ThriftFrameCodec)
        pipeline.addLast("byteEncoder", new ThriftServerChannelBufferEncoder)
        pipeline.addLast("byteDecoder", new ThriftChannelBufferDecoder)
        pipeline
      }
    }

  val serverPipelineFactory = clientPipelineFactory

  override def wrapServerChannel(service: Service[Array[Byte], Array[Byte]]) =
    (new ThriftServerTracingFilter) andThen service
}
