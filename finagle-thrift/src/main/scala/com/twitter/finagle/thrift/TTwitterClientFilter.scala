package com.twitter.finagle.thrift

import com.twitter.finagle.context.Contexts
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.util.ByteArrays
import com.twitter.finagle.Service
import com.twitter.finagle.SimpleFilter
import com.twitter.finagle.Dtab
import com.twitter.finagle.Dentry
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
 * - Request contexts
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
    extends SimpleFilter[ThriftClientRequest, Array[Byte]] {

  private[this] val clientIdBuf = clientId.map(id => Buf.Utf8(id.name))

  /**
   * Produces an upgraded TTwitter ThriftClientRequest based on Trace,
   * ClientId, and Dtab state.
   */
  private[this] def mkTTwitterRequest(baseRequest: ThriftClientRequest): ThriftClientRequest = {
    val header = new thrift.RequestHeader

    val overriddenClientId = ClientId.overridden
    val finalClientId = overriddenClientId.orElse(clientId)

    finalClientId match {
      case Some(clientId) =>
        header.setClient_id(new thrift.ClientId(clientId.name))
      case None =>
    }

    val traceId = Trace.id
    header.setSpan_id(traceId.spanId.toLong)
    traceId._parentId.foreach { id => header.setParent_span_id(id.toLong) }
    header.setTrace_id(traceId.traceId.toLong)
    header.setFlags(traceId.flags.toLong)

    traceId.sampled match {
      case Some(s) => header.setSampled(s)
      case None => header.unsetSampled()
    }

    val contexts = Contexts.broadcast.marshal().iterator
    val ctxs = new ArrayList[thrift.RequestContext]()
    if (contexts.hasNext) {
      while (contexts.hasNext) {
        val (k, buf) = contexts.next()

        // Note: we need to skip the caller-provided client id here,
        // since the existing value is derived from whatever code
        // calls into here. This should never happen in practice;
        // however if the ClientIdContext handler failed to load for
        // some reason, a pass-through context would be used instead.
        if (k != ClientId.clientIdCtx.marshalId) {
          val c = new thrift.RequestContext(
            Buf.ByteBuffer.Owned.extract(k),
            Buf.ByteBuffer.Owned.extract(buf)
          )
          ctxs.add(c)
        }
      }
    }

    val finalClientIdBuf = overriddenClientId
      .map(id => Buf.Utf8(id.name))
      .orElse(clientIdBuf)

    finalClientIdBuf match {
      case Some(buf) =>
        val ctx = new thrift.RequestContext(
          Buf.ByteBuffer.Owned.extract(ClientId.clientIdCtx.marshalId),
          Buf.ByteBuffer.Owned.extract(buf)
        )
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
        baseRequest.message
      ),
      baseRequest.oneway
    )
  }

  def apply(
    request: ThriftClientRequest,
    service: Service[ThriftClientRequest, Array[Byte]]
  ): Future[Array[Byte]] = {
    // Create a new span identifier for this request.
    val msg = new InputBuffer(request.message, protocolFactory)().readMessageBegin()
    Trace.recordRpc(msg.name)

    val thriftRequest =
      if (isUpgraded)
        mkTTwitterRequest(request)
      else
        request

    val reply = service(thriftRequest)

    if (thriftRequest.oneway) {
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
