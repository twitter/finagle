package com.twitter.finagle.thrift

import org.jboss.netty.channel.{
  SimpleChannelHandler, Channel, ChannelEvent, ChannelHandlerContext,
  SimpleChannelDownstreamHandler, MessageEvent, Channels,
  ChannelPipelineFactory}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder

import org.apache.thrift.protocol.{TBinaryProtocol, TMessage, TMessageType}
import org.apache.thrift.transport.{TMemoryBuffer, TMemoryInputTransport}

import com.twitter.util.Future

import com.twitter.finagle._
import com.twitter.finagle.util.{Ok, Error, Cancelled}
import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.channel.ChannelService

case class ThriftClientRequest(message: Array[Byte], oneway: Boolean)

class ThriftClientChannelBufferEncoder extends SimpleChannelDownstreamHandler {
  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) {
    e.getMessage match {
      case ThriftClientRequest(message, oneway) =>
        Channels.write(ctx, e.getFuture, ChannelBuffers.wrappedBuffer(message))
        if (oneway) {
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
        throw new IllegalArgumentException("no byte array")
    }
  }
}

object ThriftClientFramedCodec {
  def apply() = new ThriftClientFramedCodec
}

class ThriftClientFramedCodec extends Codec[ThriftClientRequest, Array[Byte]] {
  val clientPipelineFactory =
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("thriftFrameCodec", new ThriftFrameCodec)
        pipeline.addLast("byteEncoder", new ThriftClientChannelBufferEncoder)
        pipeline.addLast("byteDecoder", new ThriftChannelBufferDecoder)
        pipeline
      }
    }

  val serverPipelineFactory = clientPipelineFactory

  override def prepareClientChannel(underlying: Service[ThriftClientRequest, Array[Byte]]) = {
    // Attempt to upgrade the protocol the first time around by
    // sending a magic method invocation.
    val memoryBuffer = new TMemoryBuffer(512)
    val protocolFactory = new TBinaryProtocol.Factory()
    val oprot = protocolFactory.getProtocol(memoryBuffer)
    oprot.writeMessageBegin(
      new TMessage(Tracing.CanTraceMethodName, TMessageType.CALL, 0))
    val args = new CanTwitterTrace.can_twitter_trace_args()
    args.write(oprot)
    oprot.writeMessageEnd()
    oprot.getTransport().flush()
 
    val message = java.util.Arrays.copyOfRange(
      memoryBuffer.getArray(), 0, memoryBuffer.length())
    
    underlying(ThriftClientRequest(message, false)) map { reply =>
      val memoryTransport = new TMemoryInputTransport(reply)
      val iprot = protocolFactory.getProtocol(memoryTransport)
      val msg = iprot.readMessageBegin()
      if (msg.`type` == TMessageType.EXCEPTION)
        underlying
      else
        (new ThriftClientTracingFilter) andThen underlying
    }
  }
}

class ThriftClientTracingFilter extends SimpleFilter[ThriftClientRequest, Array[Byte]]
{
  def apply(request: ThriftClientRequest,
            service: Service[ThriftClientRequest, Array[Byte]]) = {
    val message = Tracing.encodeHeader(Transaction.get(), request.message)
    val tracedRequest = ThriftClientRequest(message, request.oneway)
    service(tracedRequest)
  }
}
