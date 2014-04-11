package com.twitter.finagle.thrift

import com.twitter.finagle._
import com.twitter.finagle.tracing.{Trace, Annotation}
import com.twitter.finagle.netty3.Conversions._
import com.twitter.finagle.netty3.{Ok, Error, Cancelled}
import com.twitter.finagle.util.ByteArrays
import com.twitter.io.Buf
import org.apache.thrift.protocol.{
  TBinaryProtocol, TMessage, TMessageType, TProtocolFactory}
import org.apache.thrift.transport.TMemoryInputTransport
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.{
  ChannelHandlerContext, ChannelPipelineFactory, Channels, MessageEvent,
  SimpleChannelDownstreamHandler}
import java.util.ArrayList

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
  def apply(clientId: Option[ClientId] = None) = new ThriftClientFramedCodecFactory(clientId)

  def get() = apply()
}

class ThriftClientFramedCodecFactory(
    clientId: Option[ClientId],
    _useCallerSeqIds: Boolean,
    _protocolFactory: TProtocolFactory)
  extends CodecFactory[ThriftClientRequest, Array[Byte]]#Client
{
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
    ThriftFramedTransportPipelineFactory

  override def prepareConnFactory(
    underlying: ServiceFactory[ThriftClientRequest, Array[Byte]]
  ) = preparer.prepare(underlying)
}

private case class ThriftClientPreparer(
  protocolFactory: TProtocolFactory,
  serviceName: String = "unknown",
  clientId: Option[ClientId] = None,
  useCallerSeqIds: Boolean = false) {

  def prepare(
    underlying: ServiceFactory[ThriftClientRequest, Array[Byte]]
  ) = underlying flatMap { service =>
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
      val ttwitter = new TTwitterFilter(
        serviceName,
        reply.`type` != TMessageType.EXCEPTION,
        clientId, protocolFactory)
      val seqIdFilter =
        if (protocolFactory.isInstanceOf[TBinaryProtocol.Factory] && !useCallerSeqIds)
          new SeqIdFilter
        else
          Filter.identity[ThriftClientRequest, Array[Byte]]

      val filtered = seqIdFilter andThen ttwitter andThen service
      new ValidateThriftService(filtered, protocolFactory)
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
 * TTwitterFilter implements the upnegotiated TTwitter transport, which
 * has some additional features beyond TFramed:
 *
 * - Dapper-style RPC tracing
 * - Passing client IDs
 * - Request contexts
 * - Name delegation
 *
 * @param isUpgraded Whether this connection is with a server that
 * has been upgraded to TTwitter
 */
private[thrift] class TTwitterFilter(
    serviceName: String,
    isUpgraded: Boolean,
    clientId: Option[ClientId],
    protocolFactory: TProtocolFactory)
  extends SimpleFilter[ThriftClientRequest, Array[Byte]]
{
  /**
   * Produces an upgraded TTwitter ThriftClientRequest based on Trace,
   * ClientId, and Dtab state.
   */
  private[this] def mkTTwitterRequest(
    baseRequest: ThriftClientRequest
  ): ThriftClientRequest = {
    val header = new thrift.RequestHeader

    clientId match {
      case opt@Some(clientId) =>
        header.setClient_id(clientId.toThrift)
        ClientId.set(opt)

      case None => ClientId.clear()
    }

    header.setSpan_id(Trace.id.spanId.toLong)
    Trace.id._parentId foreach { id => header.setParent_span_id(id.toLong) }
    header.setTrace_id(Trace.id.traceId.toLong)
    header.setFlags(Trace.id.flags.toLong)

    Trace.id.sampled match {
      case Some(s) => header.setSampled(s)
      case None => header.unsetSampled()
    }

    val contexts = Context.emit().iterator
    if (contexts.hasNext) {
      val ctxs = new ArrayList[thrift.RequestContext]()
      var i = 0
      while (contexts.hasNext) {
        val (k, buf) = contexts.next()

        // CSL-863: Pending further review of the Context mechanism's use of
        // ThreadLocals, we supply the constructor-set ClientId as the value
        // passed to ClientIdContext.
        val vBuf: Buf =
          (k == ClientIdContext.Key, clientId) match {
            case (true, Some(clientId)) => Buf.Utf8(clientId.name)
            case _ => buf
          }

        val c = new thrift.RequestContext(Buf.toByteBuffer(k), Buf.toByteBuffer(vBuf))
        ctxs.add(i, c)
        i += 1
      }

      header.setContexts(ctxs)
    }

    // Hygiene: Clear the current ClientId once contexts have been emitted.
    ClientId.clear()

    val dtab = Dtab.baseDiff()
    if (dtab.nonEmpty) {
      val delegations = new ArrayList[thrift.Delegation](dtab.size)
      for (Dentry(src, dst) <- dtab)
        delegations.add(new thrift.Delegation(src, dst.reified))

      header.setDelegations(delegations)
    }

    new ThriftClientRequest(
      ByteArrays.concat(
        OutputBuffer.messageToArray(header, protocolFactory),
        baseRequest.message
      ),
      baseRequest.oneway
    )
  }

  def apply(
    request: ThriftClientRequest,
    service: Service[ThriftClientRequest, Array[Byte]]
  ) = {
    // Create a new span identifier for this request.
    val msg = new InputBuffer(request.message, protocolFactory)().readMessageBegin()
    Trace.recordRpcname(serviceName, msg.name)

    val thriftRequest =
      if (isUpgraded)
        mkTTwitterRequest(request)
      else
        request

    Trace.record(Annotation.ClientSend())
    val reply = service(thriftRequest)

    if (thriftRequest.oneway) {
      // Oneway requests don't contain replies, so they can't be traced.
      reply
    } else {
      reply map { response =>
        Trace.record(Annotation.ClientRecv())

        if (isUpgraded) {
          // Peel off the ResponseHeader.
          InputBuffer.peelMessage(response, new thrift.ResponseHeader, protocolFactory)
        } else
          response
      }
    }
  }
}
