package com.twitter.finagle.thrift

import collection.JavaConversions._

import org.apache.thrift.protocol.{TBinaryProtocol, TMessage, TMessageType}
import org.jboss.netty.channel.{
  ChannelHandlerContext,
  SimpleChannelDownstreamHandler, MessageEvent, Channels,
  ChannelPipelineFactory}
import org.jboss.netty.buffer.ChannelBuffers
import java.net.SocketAddress

import com.twitter.util.Future
import com.twitter.finagle._
import com.twitter.finagle.tracing.{Trace, Event, SpanId, Endpoint}

import conversions._

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

  override def prepareService(service: Service[Array[Byte], Array[Byte]]) =
    Future.value((new ThriftServerTracingFilter(config.serviceName, config.boundAddress)) andThen service)
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

private[thrift] class ThriftServerTracingFilter
(
  serviceName: Option[String], boundAddress: SocketAddress
) extends SimpleFilter[Array[Byte], Array[Byte]]
{
  // Concurrency is not an issue here since we have an instance per
  // channel, and receive only one request at a time (thrift does no
  // pipelining). Furthermore, finagle will guarantee this by
  // serializing requests.
  private[this] var isUpgraded = false

  def apply(request: Array[Byte], service: Service[Array[Byte], Array[Byte]]) = {
    // What to do on exceptions here?
    if (isUpgraded) {
      val header = new thrift.TracedRequestHeader
      val request_ = InputBuffer.peelMessage(request, header)

      val msg = new InputBuffer(request_)().readMessageBegin()
      Trace.startSpan(
        Some(SpanId(header.getSpan_id)),
        if (header.isSetParent_span_id) Some(SpanId(header.getParent_span_id)) else None,
        Some(SpanId(header.getTrace_id)),
        serviceName,
        Some(msg.name),
        Some(Endpoint.fromSocketAddress(boundAddress).boundEndpoint))

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
        Trace.startSpan(
          None,
          None,
          None,
          serviceName,
          Some(msg.name),
          Some(Endpoint.fromSocketAddress(boundAddress).boundEndpoint))
        Trace.record(Event.ServerRecv())

        service(request) map { response =>
          Trace.record(Event.ServerSend())
          response
        }
      }
    }
  }
}

