package com.twitter.finagle.thrift

import com.twitter.finagle.tracing.{Trace, TraceId, Annotation, SpanId, Flags}
import com.twitter.finagle.util.ByteArrays
import com.twitter.finagle.{Service, SimpleFilter, Path, NameTree, Dtab, Dentry, Context}
import com.twitter.io.Buf
import com.twitter.util.Future
import org.apache.thrift.protocol.{TMessage, TMessageType, TProtocolFactory}

private[finagle] class TTwitterServerFilter(
  serviceName: String,
  protocolFactory: TProtocolFactory
) extends SimpleFilter[Array[Byte], Array[Byte]] {
  // Concurrency is not an issue here since we have an instance per
  // channel, and receive only one request at a time (thrift does no
  // pipelining).  Furthermore, finagle will guarantee this by
  // serializing requests. There are no guarantees about thread-pinning
  // however.
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

  def apply(request: Array[Byte],
      service: Service[Array[Byte], Array[Byte]]): Future[Array[Byte]] = {
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
            Dtab.local += Dentry(src, dst)
          }
        }
      }

      val msg = new InputBuffer(request_, protocolFactory)().readMessageBegin()
      Trace.recordRpc(msg.name)

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
      Trace.recordBinary("srv/thrift/clientId", ClientId.current.getOrElse("None"))

      service(request_) map {
        case response if response.isEmpty => response
        case response =>
          val responseHeader = new thrift.ResponseHeader
          ByteArrays.concat(
            OutputBuffer.messageToArray(responseHeader, protocolFactory),
            response)
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
        Trace.recordRpc(msg.name)
        Trace.recordBinary("srv/thrift/ttwitter", false)
        service(request)
      }
    }
  }

  private[this] def extractClientId(header: thrift.RequestHeader) = {
    Option(header.client_id) map { clientId => ClientId(clientId.name) }
  }
}
