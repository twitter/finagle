package com.twitter.finagle.thrift

import collection.JavaConversions._

import org.apache.thrift.protocol.{TMessage, TMessageType}
import org.jboss.netty.channel.{
  ChannelHandlerContext,
  SimpleChannelDownstreamHandler, MessageEvent, Channels,
  ChannelPipelineFactory}
import org.jboss.netty.buffer.ChannelBuffers
import java.net.InetSocketAddress

import com.twitter.util.Future
import com.twitter.finagle._
import com.twitter.finagle.tracing.{Trace, Annotation, TraceId, SpanId}
import com.twitter.finagle.util.ByteArrays
import org.apache.thrift.{TApplicationException, TException}

object ThriftServerFramedCodec {
  def apply() = new ThriftServerFramedCodecFactory
  def get()   = apply()
}

class ThriftServerFramedCodecFactory extends CodecFactory[Array[Byte], Array[Byte]]#Server {
  def apply(config: ServerCodecConfig) =
    new ThriftServerFramedCodec(config)
}

class ThriftServerFramedCodec(config: ServerCodecConfig) extends Codec[Array[Byte], Array[Byte]] {
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

  override def prepareConnFactory(factory: ServiceFactory[Array[Byte], Array[Byte]]) = {
    val boundAddress = config.boundAddress match {
      case ia: InetSocketAddress => ia
      case _ => new InetSocketAddress(0)
    }
    val trace = new ThriftServerTracingFilter(config.serviceName, boundAddress)
    trace andThen HandleUncaughtApplicationExceptions andThen factory
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
      case array: Array[Byte] =>
        e.getFuture.setSuccess()
      case _ => throw new IllegalArgumentException("no byte array")
    }
  }
}

private[thrift] object HandleUncaughtApplicationExceptions
  extends SimpleFilter[Array[Byte], Array[Byte]]
{
  def apply(request: Array[Byte], service: Service[Array[Byte], Array[Byte]]) =
    service(request) handle {
      case e if !e.isInstanceOf[TException] =>
        // NB! This is technically incorrect for one-way calls,
        // but we have no way of knowing it here. We may
        // consider simply not supporting one-way calls at all.
        val msg = InputBuffer.readMessageBegin(request)
        val name = msg.name

        val buffer = new OutputBuffer
        buffer().writeMessageBegin(
          new TMessage(name, TMessageType.EXCEPTION, msg.seqid))

        // Note: The wire contents of the exception message differ from Apache's Thrift in that here,
        // e.toString is appended to the error message.
        val x = new TApplicationException(
          TApplicationException.INTERNAL_ERROR,
          "Internal error processing " + name + ": '" + e + "'")

        x.write(buffer())
        buffer().writeMessageEnd()
        buffer.toArray
    }
  }

private[thrift] class ThriftServerTracingFilter(
  serviceName: String,
  boundAddress: InetSocketAddress
) extends SimpleFilter[Array[Byte], Array[Byte]]
{
  // Concurrency is not an issue here since we have an instance per
  // channel, and receive only one request at a time (thrift does no
  // pipelining).  Furthermore, finagle will guarantee this by
  // serializing requests.
  private[this] var isUpgraded = false

  private[this] lazy val successfulUpgradeReply = Future {
    val buffer = new OutputBuffer
    buffer().writeMessageBegin(
      new TMessage(ThriftTracing.CanTraceMethodName, TMessageType.REPLY, 0))
    val upgradeReply = new thrift.UpgradeReply
    upgradeReply.write(buffer())
    buffer().writeMessageEnd()

    // Note: currently there are no options, so there's no need
    // to parse them out.
    buffer.toArray
  }

  def apply(request: Array[Byte], service: Service[Array[Byte], Array[Byte]]) = Trace.unwind {
    // What to do on exceptions here?
    if (isUpgraded) {
      val header = new thrift.RequestHeader
      val request_ = InputBuffer.peelMessage(request, header)

      val msg = new InputBuffer(request_)().readMessageBegin()
      val sampled = if (header.isSetSampled) Some(header.isSampled) else None
      // if true, we trace this request. if None client does not trace, we get to decide

      val traceId = TraceId(
        if (header.isSetTrace_id) Some(SpanId(header.getTrace_id)) else None,
        if (header.isSetParent_span_id) Some(SpanId(header.getParent_span_id)) else None,
        SpanId(header.getSpan_id),
        sampled)


      Trace.setId(traceId)
      Trace.recordRpcname(serviceName, msg.name)
      Trace.recordServerAddr(boundAddress)
      Trace.record(Annotation.ServerRecv())

      try {
        ClientId.set(extractClientId(header))
        service(request_) map {
          case response if response.isEmpty => response
          case response =>
            Trace.record(Annotation.ServerSend())
            val responseHeader = new thrift.ResponseHeader
            ByteArrays.concat(OutputBuffer.messageToArray(responseHeader), response)
        }
      } finally {
        ClientId.clear()
      }

    } else {
      val buffer = new InputBuffer(request)
      val msg = buffer().readMessageBegin()

      // TODO: only try once?
      if (msg.`type` == TMessageType.CALL &&
          msg.name == ThriftTracing.CanTraceMethodName) {

        val connectionOptions = new thrift.ConnectionOptions
        connectionOptions.read(buffer())

        // upgrade & reply.
        isUpgraded = true
        successfulUpgradeReply
      } else {
        // request from client without tracing support

        Trace.recordServerAddr(boundAddress)
        Trace.recordRpcname(serviceName, msg.name)

        Trace.record(Annotation.ServerRecv())

        service(request) map { response =>
          Trace.record(Annotation.ServerSend())
          response
        }
      }
    }
  }

  private[this] def extractClientId(header: thrift.RequestHeader) = {
    Option(header.client_id) map { clientId => ClientId(clientId.name) }
  }
}

