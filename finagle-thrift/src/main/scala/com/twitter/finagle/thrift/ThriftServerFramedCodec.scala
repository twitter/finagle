package com.twitter.finagle.thrift

import com.twitter.finagle._
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.tracing._
import com.twitter.finagle.util.ByteArrays
import com.twitter.util.Future
import com.twitter.io.Buf
import java.net.InetSocketAddress
import org.apache.thrift.protocol.{
  TMessage, TMessageType, TProtocolFactory}
import org.apache.thrift.{TApplicationException, TException}
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.{
  ChannelHandlerContext, ChannelPipelineFactory, Channels, MessageEvent,
  SimpleChannelDownstreamHandler}

object ThriftServerFramedCodec {
  def apply(statsReceiver: StatsReceiver = NullStatsReceiver) =
    new ThriftServerFramedCodecFactory(statsReceiver)

  def apply(protocolFactory: TProtocolFactory) =
    new ThriftServerFramedCodecFactory(protocolFactory)

  def get() = apply()
}

class ThriftServerFramedCodecFactory(protocolFactory: TProtocolFactory)
    extends CodecFactory[Array[Byte], Array[Byte]]#Server
{
  def this(statsReceiver: StatsReceiver) =
    this(Protocols.binaryFactory(statsReceiver = statsReceiver))

  def this() = this(NullStatsReceiver)

  def apply(config: ServerCodecConfig) =
    new ThriftServerFramedCodec(config, protocolFactory)
}

class ThriftServerFramedCodec(
    config: ServerCodecConfig,
    protocolFactory: TProtocolFactory = Protocols.binaryFactory()
) extends Codec[Array[Byte], Array[Byte]] {
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

  private[this] val boundAddress = config.boundAddress match {
    case ia: InetSocketAddress => ia
    case _ => new InetSocketAddress(0)
  }

  private[this] val preparer = ThriftServerPreparer(
    protocolFactory, config.serviceName, boundAddress)

  override def prepareConnFactory(factory: ServiceFactory[Array[Byte], Array[Byte]]) =
    preparer.prepare(factory)
}

private case class ThriftServerPreparer(
  protocolFactory: TProtocolFactory,
  serviceName: String,
  boundAddress: InetSocketAddress) {

  private[this] val uncaughtExceptionsFilter =
    new HandleUncaughtApplicationExceptions(protocolFactory)

  def prepare(
    factory: ServiceFactory[Array[Byte], Array[Byte]]
  ): ServiceFactory[Array[Byte], Array[Byte]] = factory map { service =>
    val trace = new ThriftServerTracingFilter(
      serviceName, boundAddress, protocolFactory)
    trace andThen uncaughtExceptionsFilter andThen service
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

private[finagle]
class HandleUncaughtApplicationExceptions(protocolFactory: TProtocolFactory)
  extends SimpleFilter[Array[Byte], Array[Byte]]
{
  def apply(request: Array[Byte], service: Service[Array[Byte], Array[Byte]]) =
    service(request) handle {
      case e if !e.isInstanceOf[TException] =>
        // NB! This is technically incorrect for one-way calls,
        // but we have no way of knowing it here. We may
        // consider simply not supporting one-way calls at all.
        val msg = InputBuffer.readMessageBegin(request, protocolFactory)
        val name = msg.name

        val buffer = new OutputBuffer(protocolFactory)
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
  boundAddress: InetSocketAddress,
  protocolFactory: TProtocolFactory
) extends SimpleFilter[Array[Byte], Array[Byte]]
{
  // Concurrency is not an issue here since we have an instance per
  // channel, and receive only one request at a time (thrift does no
  // pipelining).  Furthermore, finagle will guarantee this by
  // serializing requests.
  private[this] var isUpgraded = false

  private[this] lazy val successfulUpgradeReply = Future {
    val buffer = new OutputBuffer(protocolFactory)
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
      val request_ = InputBuffer.peelMessage(request, header, protocolFactory)

      // Set the TraceId. This will be overwritten by TraceContext, if it is
      // loaded, but it should never be the case that the ids from the two
      // paths won't match.
      val sampled = if (header.isSetSampled) Some(header.isSampled) else None
      // if true, we trace this request. if None client does not trace, we get to decide

      val traceId = TraceId(
        if (header.isSetTrace_id)
          Some(SpanId(header.getTrace_id)) else None,
        if (header.isSetParent_span_id)
          Some(SpanId(header.getParent_span_id)) else None,
        SpanId(header.getSpan_id),
        sampled,
        if (header.isSetFlags) Flags(header.getFlags) else Flags()
      )

      Trace.setId(traceId)

      // Destination is ignored for now,
      // as it really requires a dispatcher.
      if (header.getDelegationsSize() > 0) {
        val ds = header.getDelegationsIterator()
        while (ds.hasNext()) {
          val d = ds.next()
          if (d.src != null && d.dst != null) {
            val src = Path.read(d.src)
            val dst = NameTree.read(d.dst)
            Dtab.delegate(Dentry(src, dst))
          }
        }
      }

      val msg = new InputBuffer(request_, protocolFactory)().readMessageBegin()
      Trace.recordServiceName(serviceName)
      Trace.recordRpc(msg.name)
      Trace.record(Annotation.ServerRecv())

      if (header.contexts != null) {
        val iter = header.contexts.iterator()
        while (iter.hasNext) {
          val c = iter.next()
          Context.handle(Buf.ByteArray(c.getKey()), Buf.ByteArray(c.getValue()))
        }
      }

      // If `header.client_id` field is non-null, then allow it to take
      // precedence over the id provided by ClientIdContext.
      extractClientId(header) foreach { clientId => ClientId.set(Some(clientId)) }

      try {
        service(request_) map {
          case response if response.isEmpty => response
          case response =>
            Trace.record(Annotation.ServerSend())
            val responseHeader = new thrift.ResponseHeader
            ByteArrays.concat(
              OutputBuffer.messageToArray(responseHeader, protocolFactory),
              response)
        }
      } finally {
        ClientId.clear()
        Dtab.clear()
      }
    } else {
      val buffer = new InputBuffer(request, protocolFactory)
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

        Trace.recordServiceName(serviceName)
        Trace.recordRpc(msg.name)

        Trace.record(Annotation.ServerRecv())
        Trace.record("finagle.thrift.noUpgrade")

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
