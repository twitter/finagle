package com.twitter.finagle.thrift

import com.twitter.finagle._
import com.twitter.finagle.filter.PayloadSizeFilter
import com.twitter.util.{Future, Stopwatch}
import org.apache.thrift.protocol.{TBinaryProtocol, TMessage, TMessageType, TProtocolFactory}
import org.apache.thrift.transport.TMemoryInputTransport
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel._

/**
 * ThriftClientFramedCodec implements a framed thrift transport that
 * supports upgrading in order to provide TraceContexts across
 * requests.
 */
object ThriftClientFramedCodec {
  /**
   * Create a [[com.twitter.finagle.thrift.ThriftClientFramedCodecFactory]].
   * Passing a ClientId will propagate that information to the server iff the server is a finagle
   * server.
   */
  def apply(clientId: Option[ClientId] = None) =
    new ThriftClientFramedCodecFactory(clientId)

  def get() = apply()
}

class ThriftClientFramedCodecFactory(
    clientId: Option[ClientId],
    _useCallerSeqIds: Boolean,
    _protocolFactory: TProtocolFactory)
  extends CodecFactory[ThriftClientRequest, Array[Byte]]#Client {

  def this(clientId: Option[ClientId]) = this(clientId, false, Protocols.binaryFactory())

  def this(clientId: ClientId) = this(Some(clientId))

  // Fix this after the API/ABI freeze (use case class builder)
  def useCallerSeqIds(x: Boolean): ThriftClientFramedCodecFactory =
    new ThriftClientFramedCodecFactory(clientId, x, _protocolFactory)

  /**
   * Use the given protocolFactory in stead of the default `TBinaryProtocol.Factory`
   */
  def protocolFactory(pf: TProtocolFactory) =
    new ThriftClientFramedCodecFactory(clientId, _useCallerSeqIds, pf)

  /**
   * Create a [[com.twitter.finagle.thrift.ThriftClientFramedCodec]]
   * with a default TBinaryProtocol.
   */
  def apply(config: ClientCodecConfig) =
    new ThriftClientFramedCodec(_protocolFactory, config, clientId, _useCallerSeqIds)
}

class ThriftClientFramedCodec(
  protocolFactory: TProtocolFactory,
  config: ClientCodecConfig,
  clientId: Option[ClientId] = None,
  useCallerSeqIds: Boolean = false
) extends Codec[ThriftClientRequest, Array[Byte]] {

  private[this] val preparer = ThriftClientPreparer(
    protocolFactory, config.serviceName,
    clientId, useCallerSeqIds)

  def pipelineFactory: ChannelPipelineFactory =
    ThriftClientFramedPipelineFactory

  override def prepareConnFactory(
    underlying: ServiceFactory[ThriftClientRequest, Array[Byte]],
    params: Stack.Params
  ) = preparer.prepare(underlying, params)

  override val protocolLibraryName: String = "thrift"
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
          e.getFuture.addListener(new ChannelFutureListener {
            override def operationComplete(f: ChannelFuture): Unit =
              if (f.isSuccess) {
                Channels.fireMessageReceived(ctx, ChannelBuffers.EMPTY_BUFFER)
              } else if (f.isCancelled) {
                Channels.fireExceptionCaught(ctx, new CancelledRequestException)
              } else {
                Channels.fireExceptionCaught(ctx, f.getCause)
              }
          })
        }

      case _ =>
        throw new IllegalArgumentException("No ThriftClientRequest on the wire")
    }
}

/**
 * A class to prepare a client. It adds a payload size filter and a
 * connection validation filter. If requested, it will also prepare
 * clients for upgrade: it attempts to send a probe message to upgrade
 * the protocol to TTwitter. If this succeeds, the TTwitter filter is
 * added.
 */
private[finagle] case class ThriftClientPreparer(
    protocolFactory: TProtocolFactory,
    serviceName: String = "unknown",
    clientId: Option[ClientId] = None,
    useCallerSeqIds: Boolean = false) {

  def prepareService(params: Stack.Params)(
    service: Service[ThriftClientRequest, Array[Byte]]
  ): Future[Service[ThriftClientRequest, Array[Byte]]] = {
    val payloadSize = new PayloadSizeFilter[ThriftClientRequest, Array[Byte]](
      params[param.Stats].statsReceiver, _.message.length, _.length
    )
    val Thrift.param.AttemptTTwitterUpgrade(attemptUpgrade) =
      params[Thrift.param.AttemptTTwitterUpgrade]
    val payloadSizeService = payloadSize.andThen(service)
    val upgradedService =
      if (attemptUpgrade) {
        upgrade(payloadSizeService)
      } else {
        Future.value(payloadSizeService)
      }

    upgradedService.map { upgraded =>
      new ValidateThriftService(upgraded, protocolFactory)
    }
  }

  def prepare(
    underlying: ServiceFactory[ThriftClientRequest, Array[Byte]],
    params: Stack.Params
  ): ServiceFactory[ThriftClientRequest, Array[Byte]] = {
    val param.Stats(stats) = params[param.Stats]
    val Thrift.param.AttemptTTwitterUpgrade(attemptUpgrade) =
      params[Thrift.param.AttemptTTwitterUpgrade]
    val preparingFactory = underlying.flatMap(prepareService(params))

    if (attemptUpgrade) {
      new ServiceFactoryProxy(preparingFactory) {
        val stat = stats.stat("codec_connection_preparation_latency_ms")
        override def apply(conn: ClientConnection) = {
          val elapsed = Stopwatch.start()
          super.apply(conn).ensure {
            stat.add(elapsed().inMilliseconds)
          }
        }
      }
    } else {
      preparingFactory
    }
  }

  private def upgrade(
    service: Service[ThriftClientRequest, Array[Byte]]
  ): Future[Service[ThriftClientRequest, Array[Byte]]] = {
    // Attempt to upgrade the protocol the first time around by
    // sending a magic method invocation.
    val buffer = new OutputBuffer(protocolFactory)
    buffer().writeMessageBegin(
      new TMessage(ThriftTracing.CanTraceMethodName, TMessageType.CALL, 0))

    val options = new thrift.ConnectionOptions
    options.write(buffer())

    buffer().writeMessageEnd()

    service(new ThriftClientRequest(buffer.toArray, false)) map { bytes =>
      val memoryTransport = new TMemoryInputTransport(bytes)
      val iprot = protocolFactory.getProtocol(memoryTransport)
      val reply = iprot.readMessageBegin()

      val ttwitter = new TTwitterClientFilter(
        serviceName,
        reply.`type` != TMessageType.EXCEPTION,
        clientId, protocolFactory)
      // TODO: also apply this for Protocols.binaryFactory

      val seqIdFilter =
        if (protocolFactory.isInstanceOf[TBinaryProtocol.Factory] && !useCallerSeqIds)
          new SeqIdFilter
        else
          Filter.identity[ThriftClientRequest, Array[Byte]]

      seqIdFilter.andThen(ttwitter).andThen(service)
    }
  }
}

/**
 * A Netty ChannelPipelineFactory for framing and deframing thrift messages
  */
private[finagle]
object ThriftClientFramedPipelineFactory extends ChannelPipelineFactory {
  def getPipeline() = {
    val pipeline = Channels.pipeline()
    pipeline.addLast("thriftFrameCodec", new ThriftFrameCodec)
    pipeline.addLast("byteEncoder",      new ThriftClientChannelBufferEncoder)
    pipeline.addLast("byteDecoder",      new ThriftChannelBufferDecoder)
    pipeline
  }
}
