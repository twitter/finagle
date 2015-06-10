package com.twitter.finagle.thrift

import com.twitter.finagle._
import com.twitter.finagle.service.RetryPolicy
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.tracing.TraceInitializerFilter
import com.twitter.io.Buf
import com.twitter.util.Future
import org.apache.thrift.protocol.{TMessage, TMessageType, TProtocolFactory}
import org.apache.thrift.{TApplicationException, TException}
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.{
  ChannelHandlerContext, ChannelPipelineFactory, Channels, MessageEvent,
  SimpleChannelDownstreamHandler}

private[finagle] object ThriftServerFramedPipelineFactory  extends ChannelPipelineFactory {
  def getPipeline() = {
    val pipeline = Channels.pipeline()
    pipeline.addLast("thriftFrameCodec", new ThriftFrameCodec)
    pipeline.addLast("byteEncoder", new ThriftServerChannelBufferEncoder)
    pipeline.addLast("byteDecoder", new ThriftChannelBufferDecoder)
    pipeline
  }
}

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
  def pipelineFactory: ChannelPipelineFactory = ThriftServerFramedPipelineFactory

  private[this] val preparer = ThriftServerPreparer(
    protocolFactory, config.serviceName)

  override def prepareConnFactory(factory: ServiceFactory[Array[Byte], Array[Byte]]) =
    preparer.prepare(factory)

  override def newTraceInitializer = TraceInitializerFilter.serverModule[Array[Byte], Array[Byte]]

  override val protocolLibraryName: String = "thrift"
}

private[finagle] case class ThriftServerPreparer(
    protocolFactory: TProtocolFactory,
    serviceName: String) {
  private[this] val uncaughtExceptionsFilter =
    new UncaughtAppExceptionFilter(protocolFactory)

  def prepare(
    factory: ServiceFactory[Array[Byte], Array[Byte]]
  ): ServiceFactory[Array[Byte], Array[Byte]] = factory map { service =>
    val ttwitter = new TTwitterServerFilter(serviceName, protocolFactory)
    ttwitter andThen uncaughtExceptionsFilter andThen service
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

private[finagle] object UncaughtAppExceptionFilter {

  /**
   * Creates a Thrift exception message for the given `exception` and thrift `thriftRequest`
   * message using the given [[org.apache.thrift.protocol.TProtocolFactory]].
   */
  def writeExceptionMessage(
    thriftRequest: Buf,
    throwable: Throwable,
    protocolFactory: TProtocolFactory
  ): Buf = {
    val reqBytes = Buf.ByteArray.Owned.extract(thriftRequest)
    // NB! This is technically incorrect for one-way calls,
    // but we have no way of knowing it here. We may
    // consider simply not supporting one-way calls at all.
    val msg = InputBuffer.readMessageBegin(reqBytes, protocolFactory)
    val name = msg.name

    val buffer = new OutputBuffer(protocolFactory)
    buffer().writeMessageBegin(
      new TMessage(name, TMessageType.EXCEPTION, msg.seqid))

    // Note: The wire contents of the exception message differ from Apache's Thrift in that here,
    // e.toString is appended to the error message.
    val x = new TApplicationException(
      TApplicationException.INTERNAL_ERROR,
      s"Internal error processing $name: '$throwable'")

    x.write(buffer())
    buffer().writeMessageEnd()
    Buf.ByteArray.Owned(buffer.toArray)
  }

}

private[finagle] class UncaughtAppExceptionFilter(protocolFactory: TProtocolFactory)
  extends SimpleFilter[Array[Byte], Array[Byte]]
{
  import UncaughtAppExceptionFilter.writeExceptionMessage

  def apply(
    request: Array[Byte],
    service: Service[Array[Byte], Array[Byte]]
  ): Future[Array[Byte]] =
    service(request).handle {
      case e if !e.isInstanceOf[TException] =>
        val buf = Buf.ByteArray.Owned(request)
        val msg = writeExceptionMessage(buf, e, protocolFactory)
        Buf.ByteArray.Owned.extract(msg)
    }
}
