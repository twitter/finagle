package com.twitter.finagle.thrift

import com.twitter.finagle.context.Contexts
import com.twitter.finagle.tracing.{Trace, Annotation}
import com.twitter.finagle.util.ByteArrays
import com.twitter.finagle.{Service, SimpleFilter, Dtab, Dentry}
import com.twitter.io.Buf
import com.twitter.util.Future
import java.util.ArrayList
import org.apache.thrift.protocol.TProtocolFactory

/**
 * TTwitterFilter implements the upnegotiated TTwitter transport, which
 * has some additional features beyond TFramed:
 *
 * - Dapper-style RPC tracing
 * - Passing client IDs
 * - Ask contexts
 * - Name delegation
 *
 * @param isUpgraded Whether this connection is with a server that
 * has been upgraded to TTwitter
 */
private[thrift] class TTwitterClientFilter(
    serviceName: String,
    isUpgraded: Boolean,
    clientId: Option[ClientId],
    protocolFactory: TProtocolFactory)
  extends SimpleFilter[ThriftClientRequest, Array[Byte]]
{
  private[this] val clientIdBuf = clientId map { id => Buf.Utf8(id.name) }

  /**
   * Produces an upgraded TTwitter ThriftClientRequest based on Trace,
   * ClientId, and Dtab state.
   */
  private[this] def mkTTwitterAsk(baseAsk: ThriftClientRequest): ThriftClientRequest = {
    val header = new thrift.AskHeader

    clientId match {
      case Some(clientId) =>
        header.setClient_id(clientId.toThrift)
      case None =>
    }

    header.setSpan_id(Trace.id.spanId.toLong)
    Trace.id._parentId foreach { id => header.setParent_span_id(id.toLong) }
    header.setTrace_id(Trace.id.traceId.toLong)
    header.setFlags(Trace.id.flags.toLong)

    Trace.id.sampled match {
      case Some(s) => header.setSampled(s)
      case None => header.unsetSampled()
    }

    val contexts = Contexts.broadcast.marshal().iterator
    val ctxs = new ArrayList[thrift.AskContext]()
    if (contexts.hasNext) {
      while (contexts.hasNext) {
        val (k, buf) = contexts.next()

        // Note: we need to skip the caller-provided client id here,
        // since the existing value is derived from whatever code
        // calls into here. This should never happen in practice;
        // however if the ClientIdContext handler failed to load for
        // some reason, a pass-through context would be used instead.
        if (k != ClientId.clientIdCtx.marshalId) {
          val c = new thrift.AskContext(
            Buf.ByteBuffer.Owned.extract(k), Buf.ByteBuffer.Owned.extract(buf))
          ctxs.add(c)
        }
      }
    }
    clientIdBuf match {

      case Some(buf) =>
        val ctx = new thrift.AskContext(
          Buf.toByteBuffer(ClientId.clientIdCtx.marshalId), 
          Buf.toByteBuffer(buf))
        ctxs.add(ctx)
      case None => // skip
    }
    
    if (!ctxs.isEmpty)
      header.setContexts(ctxs)

    val dtab = Dtab.local
    if (dtab.nonEmpty) {
      val delegations = new ArrayList[thrift.Delegation](dtab.size)
      for (Dentry(src, dst) <- dtab)
        delegations.add(new thrift.Delegation(src.show, dst.show))

      header.setDelegations(delegations)
    }

    new ThriftClientRequest(
      ByteArrays.concat(
        OutputBuffer.messageToArray(header, protocolFactory),
        baseAsk.message
      ),
      baseAsk.oneway
    )
  }

  def apply(request: ThriftClientRequest,
      service: Service[ThriftClientRequest, Array[Byte]]): Future[Array[Byte]] = {
    // Create a new span identifier for this request.
    val msg = new InputBuffer(request.message, protocolFactory)().readMessageBegin()
    Trace.recordRpc(msg.name)

    val thriftAsk =
      if (isUpgraded)
        mkTTwitterAsk(request)
      else
        request

    val reply = service(thriftAsk)

    if (thriftAsk.oneway) {
      // Oneway requests don't contain replies, so they can't be traced.
      reply
    } else {
      reply map { response =>

        if (isUpgraded) {
          // Peel off the ResponseHeader.
          InputBuffer.peelMessage(response, new thrift.ResponseHeader, protocolFactory)
        } else
          response
      }
    }
  }
}

