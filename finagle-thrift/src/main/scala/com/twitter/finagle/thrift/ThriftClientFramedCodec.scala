package com.twitter.finagle.thrift

import collection.JavaConversions._

import org.jboss.netty.channel.{
  ChannelHandlerContext,
  SimpleChannelDownstreamHandler, MessageEvent, Channels,
  ChannelPipelineFactory}
import org.jboss.netty.buffer.ChannelBuffers
import org.apache.thrift.protocol.{
  TBinaryProtocol, TMessage,
  TMessageType, TProtocolFactory}
import org.apache.thrift.transport.TMemoryInputTransport
import com.twitter.util.Time

import com.twitter.finagle._
import com.twitter.finagle.util.{Ok, Error, Cancelled}
import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.tracing.{Trace, Annotation}

import java.net.{InetSocketAddress, SocketAddress}

/**
 * ThriftClientFramedCodec implements a framed thrift transport that
 * supports upgrading in order to provide TraceContexts across
 * requests.
 */
object ThriftClientFramedCodec {
  /**
   * Create a [[com.twitter.finagle.thrift.ThriftClientFramedCodecFactory]]
   */
  def apply() = new ThriftClientFramedCodecFactory
  def get() = apply()
}

class ThriftClientFramedCodecFactory
  extends CodecFactory[ThriftClientRequest, Array[Byte]]#Client
{
  /**
   * Create a [[com.twitter.finagle.thrift.ThriftClientFramedCodec]]
   * with a default TBinaryProtocol.
   */
  def apply(config: ClientCodecConfig) = {
    new ThriftClientFramedCodec(new TBinaryProtocol.Factory(), config)
  }
}

class ThriftClientFramedCodec(protocolFactory: TProtocolFactory, config: ClientCodecConfig)
  extends Codec[ThriftClientRequest, Array[Byte]]
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
    val options = new thrift.TraceOptions
    options.write(buffer())
    buffer().writeMessageEnd()

    underlying(new ThriftClientRequest(buffer.toArray, false)) map { bytes =>
      val memoryTransport = new TMemoryInputTransport(bytes)
      val iprot           = protocolFactory.getProtocol(memoryTransport)
      val reply           = iprot.readMessageBegin()

      (new ThriftClientTracingFilter(
        config.serviceName,
        reply.`type` != TMessageType.EXCEPTION
      )) andThen underlying
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
        ctx.getChannel.getLocalAddress()  match {
          case ia: InetSocketAddress => Trace.recordClientAddr(ia)
          case _ => () // nothing
        }

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
 *
 * @param isUpgraded Whether this connection is with a server that has tracing enabled
 */

private[thrift] class ThriftClientTracingFilter(serviceName: String, isUpgraded: Boolean)
  extends SimpleFilter[ThriftClientRequest, Array[Byte]]
{
  def apply(
    request: ThriftClientRequest,
    service: Service[ThriftClientRequest, Array[Byte]]
  ) = {
    // Create a new span identifier for this request.
    val msg = new InputBuffer(request.message)().readMessageBegin()
    Trace.recordRpcname(serviceName, msg.name)

    val thriftRequest = if (isUpgraded) {
      val header = new thrift.TracedRequestHeader
      header.setSpan_id(Trace.id.spanId.toLong)
      header.setParent_span_id(Trace.id.parentId.toLong)
      header.setTrace_id(Trace.id.traceId.toLong)

      new ThriftClientRequest(
        OutputBuffer.messageToArray(header) ++ request.message,
        request.oneway)
    } else {
      request
    }

    Trace.record(Annotation.ClientSend())
    val reply = service(thriftRequest)
    if (thriftRequest.oneway) {
      // Oneway requests don't contain replies, and so they can't be
      // traced.
      reply
    } else {
      reply map { response =>
        Trace.record(Annotation.ClientRecv())

        if (isUpgraded) {
          // Peel off the TracedResponseHeader.
          InputBuffer.peelMessage(response, new thrift.TracedResponseHeader)
        } else {
          response
        }
      }
    }
  }
}
