package com.twitter.finagle.thrift

import collection.JavaConversions._

import org.apache.thrift.protocol.{TBinaryProtocol, TMessage, TMessageType}
import org.jboss.netty.channel.{
  ChannelHandlerContext,
  SimpleChannelDownstreamHandler, MessageEvent, Channels,
  ChannelPipelineFactory}
import org.jboss.netty.buffer.ChannelBuffers
import java.net.{InetSocketAddress, SocketAddress}

import com.twitter.util.Future
import com.twitter.finagle._
import com.twitter.finagle.tracing.{Trace, Annotation, TraceId, SpanId}

object ThriftServerFramedCodec {
  def apply() = ThriftServerFramedCodecFactory
  def get() = apply()
}

object ThriftServerFramedCodecFactory
  extends CodecFactory[Array[Byte], Array[Byte]]#Server
{
  def apply(config: ServerCodecConfig) = new ThriftServerFramedCodec(config)
}

class ThriftServerFramedCodec(config: ServerCodecConfig)
  extends Codec[Array[Byte], Array[Byte]]
{
  def pipelineFactory =
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("thriftFrameCodec", new ThriftFrameCodec)
        pipeline.addLast("byteEncoder", new ThriftServerChannelBufferEncoder)
        pipeline.addLast("byteDecoder", new ThriftChannelBufferDecoder)
        pipeline
      }
    }

  override def prepareService(service: Service[Array[Byte], Array[Byte]]) = {
    val ia = config.boundAddress match {
      case ia: InetSocketAddress => ia
      case _ => new InetSocketAddress(0)
    }
    Future.value((new ThriftServerTracingFilter(config.serviceName, ia)) andThen service)
  }
}

private[thrift] class ThriftServerChannelBufferEncoder
  extends SimpleChannelDownstreamHandler
{
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

private[thrift] class ThriftServerTracingFilter(
  serviceName: String, boundAddress: InetSocketAddress
) extends SimpleFilter[Array[Byte], Array[Byte]]
{
  // Concurrency is not an issue here since we have an instance per
  // channel, and receive only one request at a time (thrift does no
  // pipelining).  Furthermore, finagle will guarantee this by
  // serializing requests.
  private[this] var isUpgraded = false

  def apply(request: Array[Byte], service: Service[Array[Byte], Array[Byte]]) = Trace.unwind {
    // What to do on exceptions here?
    if (isUpgraded) {
      val header = new thrift.TracedRequestHeader
      val request_ = InputBuffer.peelMessage(request, header)

      val msg = new InputBuffer(request_)().readMessageBegin()
      val traceId = TraceId(
        if (header.isSetTrace_id) Some(SpanId(header.getTrace_id)) else None,
        if (header.isSetParent_span_id) Some(SpanId(header.getParent_span_id)) else None,
        SpanId(header.getSpan_id))
      Trace.pushId(traceId)
      Trace.recordRpcname(serviceName, msg.name)
      Trace.recordServerAddr(boundAddress)
      Trace.record(Annotation.ServerRecv())

      service(request_) map { response =>
        Trace.record(Annotation.ServerSend())
        val responseHeader = new thrift.TracedResponseHeader
        OutputBuffer.messageToArray(responseHeader) ++ response
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

        // Note: currently there are no options, so there's no need
        // to parse them out.
        Future.value(buffer.toArray)
      } else {
        Trace.pushId()
        Trace.record(Annotation.ServerRecv())

        service(request) map { response =>
          Trace.record(Annotation.ServerSend())
          response
        }
      }
    }
  }
}

