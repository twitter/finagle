package com.twitter.finagle.thrift

import collection.JavaConversions._     // XXX

import org.apache.thrift.protocol.{TBinaryProtocol, TMessage, TMessageType}
import org.jboss.netty.channel.{
  ChannelHandlerContext,
  SimpleChannelDownstreamHandler, MessageEvent, Channels,
  ChannelPipelineFactory}
import org.jboss.netty.buffer.ChannelBuffers
import com.twitter.util.Future
import com.twitter.finagle._
import com.twitter.finagle.tracing.{Trace, Event}

import conversions._

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

object ThriftServerFramedCodec {
  def apply() = new ThriftServerFramedCodec
}

private[thrift] class ThriftServerTracingFilter
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
      val header = new thrift.TracedRequestHeader
      val request_ = InputBuffer.peelMessage(request, header)

      Trace.startSpan(
        Some(header.getSpan_id),
        Some(header.getParent_span_id),
        Some(header.getTrace_id))

      if (header.debug)
        Trace.debug(true)  // (don't turn off when !header.debug)

      Trace.record(Event.ServerRecv())

      service(request_) map { response =>
        Trace.record(Event.ServerSend())

        // Wrap some trace data.
        val responseHeader = new thrift.TracedResponseHeader

        if (header.debug) {
          // Piggy-back span data if we're in debug mode.
          Trace().toThriftSpans foreach { responseHeader.addToSpans(_) }
        }

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
        service(request)
      }
    }
  }
}

class ThriftServerFramedCodec
  extends ServerCodec[Array[Byte], Array[Byte]]
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

  override def prepareService(service: Service[Array[Byte], Array[Byte]]) =
    Future.value((new ThriftServerTracingFilter) andThen service)
}
