package com.twitter.finagle.thrift

import scala.collection.JavaConversions._
import org.jboss.netty.channel.{
  ChannelHandlerContext,
  SimpleChannelDownstreamHandler, MessageEvent, Channels,
  ChannelPipelineFactory}
import org.jboss.netty.buffer.ChannelBuffers
import org.apache.thrift.protocol.{TBinaryProtocol, TMessage, TMessageType}
import org.apache.thrift.transport.TMemoryInputTransport

import com.twitter.util.Time

import com.twitter.finagle._
import com.twitter.finagle.util.{Ok, Error, Cancelled}
import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.tracing.{TraceID, Trace, Record}

/**
 * ThriftClientFramedCodec implements a framed thrift transport that
 * supports upgrading in order to provide TraceContexts across
 * requests.
 */
object ThriftClientFramedCodec {
  def apply() = new ThriftClientFramedCodec
}

class ThriftClientFramedCodec extends ClientCodec[ThriftClientRequest, Array[Byte]]
{
  def pipelineFactory =
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("thriftFrameCodec", new ThriftFrameCodec)
        pipeline.addLast("byteEncoder",      new ThriftClientChannelBufferEncoder)
        pipeline.addLast("byteDecoder",      new ThriftChannelBufferDecoder)
        pipeline
      }
    }

  override def prepareService(underlying: Service[ThriftClientRequest, Array[Byte]]) = {
    // Attempt to upgrade the protocol the first time around by
    // sending a magic method invocation.
    val buffer = new OutputBuffer()
    buffer().writeMessageBegin(
      new TMessage(ThriftTracing.CanTraceMethodName, TMessageType.CALL, 0))
    val options = new TraceOptions
    options.write(buffer())
    buffer().writeMessageEnd()

    underlying(new ThriftClientRequest(buffer.toArray, false)) map { bytes =>
      val protocolFactory = new TBinaryProtocol.Factory()
      val memoryTransport = new TMemoryInputTransport(bytes)
      val iprot           = protocolFactory.getProtocol(memoryTransport)
      val reply           = iprot.readMessageBegin()

      if (reply.`type` == TMessageType.EXCEPTION) {
        // Return just the underlying service if we caused an
        // exception: this means the remote end didn't support
        // tracing.
        underlying
      } else {
        // Otherwise, apply our tracing filter first. This will read
        // the TraceData frames, and apply them to the current
        // Trace.
        (new ThriftClientTracingFilter) andThen underlying
      }
    }
  }
}

/**
 * ThriftClientChannelBufferEncoder translates ThriftClientRequests to
 * bytes on the wire. It satisfies the request immediately if it is a
 * "oneway" request.
 */
private[thrift] class ThriftClientChannelBufferEncoder
  extends SimpleChannelDownstreamHandler
{
  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) =
    e.getMessage match {
      case request: ThriftClientRequest =>
        Channels.write(ctx, e.getFuture, ChannelBuffers.wrappedBuffer(request.message))
        if (request.oneway) {
          // oneway RPCs are satisfied when the write is complete.
          e.getFuture() {
            case Ok(_) =>
              Channels.fireMessageReceived(ctx, ChannelBuffers.EMPTY_BUFFER)
            case Error(e) =>
              Channels.fireExceptionCaught(ctx, e)
            case Cancelled =>
              Channels.fireExceptionCaught(ctx, new CancelledRequestException)
          }
        }

      case _ =>
        throw new IllegalArgumentException("No ThriftClientRequest on the wire")
    }
}

/**
 * ThriftClientTracingFilter implements Trace support for thrift. This
 * is applied *after* the Channel has been upgraded (via
 * negotiation). It serializes the current Trace into a header
 * on the wire. It is applied after all framing.
 */

private[thrift] class ThriftClientTracingFilter extends SimpleFilter[ThriftClientRequest, Array[Byte]]
{
  def apply(request: ThriftClientRequest,
            service: Service[ThriftClientRequest, Array[Byte]]) =
  {
    val header = new TracedRequest
    header.setParent_span_id(Trace().traceID.span)
    header.setDebug(Trace().transcript.isRecording)

    val tracedRequest = new ThriftClientRequest(
      OutputBuffer.messageToArray(header) ++ request.message,
      request.oneway)

    val reply = service(tracedRequest)
    if (tracedRequest.oneway) {
      // Oneway requests don't contain replies, and so they can't be
      // traced.
      reply
    } else {
      reply map { response =>
        val responseHeader = new TracedResponse
        val rest = InputBuffer.peelMessage(response, responseHeader)

        if (header.debug && responseHeader.isSetTranscript) {
          val records = responseHeader.transcript map { thriftRecord =>
            val traceID = TraceID(
              thriftRecord.getSpan_id(),
              {
                val spanID = thriftRecord.getParent_span_id()
                if (spanID == 0) None else Some(spanID)
              },
              thriftRecord.getHost(),
              thriftRecord.getVm_id())

            Record(
              traceID,
              Time.fromMilliseconds(thriftRecord.getTimestamp_ms()),
              thriftRecord.getMessage())
          }

          Trace().transcript.merge(records.iterator)
        }

        rest
      }
    }
  }
}
