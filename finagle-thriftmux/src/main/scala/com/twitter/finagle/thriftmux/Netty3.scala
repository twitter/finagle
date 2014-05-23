package com.twitter.finagle.thriftmux

import com.twitter.finagle.{mux, Dtab, ThriftMuxUtil}
import com.twitter.finagle.mux.{BadMessageException, Message}
import com.twitter.finagle.thrift._
import com.twitter.finagle.thrift.thrift.{
  ResponseHeader, RequestContext, RequestHeader, UpgradeReply}
import com.twitter.finagle.tracing.{Flags, SpanId, TraceContext, TraceId}
import com.twitter.io.Buf
import com.twitter.util.{Try, Return, Throw, NonFatal}
import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.atomic.AtomicInteger
import org.apache.thrift.protocol.{TProtocolFactory, TMessage, TMessageType}
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import org.jboss.netty.channel._
import scala.collection.mutable.ArrayBuffer

private[finagle] class PipelineFactory(protocolFactory: TProtocolFactory)
  extends ChannelPipelineFactory
{
  case class UnexpectedRequestException(err: String) extends Exception(err)

  private object TTwitterToMux {
    private val responseHeader = ChannelBuffers.wrappedBuffer(
      OutputBuffer.messageToArray(new ResponseHeader, protocolFactory))
  }

  private class TTwitterToMux extends SimpleChannelHandler {
    import TTwitterToMux._

    private[this] def contextStructToKVTuple(c: RequestContext): (ChannelBuffer, ChannelBuffer) =
      (ChannelBuffers.wrappedBuffer(c.getKey), ChannelBuffers.wrappedBuffer(c.getValue))

    private[this] def thriftToMux(req: ChannelBuffer): Message.Tdispatch = {
      val header = new RequestHeader
      val request_ = InputBuffer.peelMessage(ThriftMuxUtil.bufferToArray(req), header, protocolFactory)
      val sampled = if (header.isSetSampled) Some(header.isSampled) else None
      val traceId = TraceId(
        if (header.isSetTrace_id) Some(SpanId(header.getTrace_id)) else None,
        if (header.isSetParent_span_id) Some(SpanId(header.getParent_span_id)) else None,
        SpanId(header.getSpan_id),
        sampled,
        if (header.isSetFlags) Flags(header.getFlags) else Flags()
      )

      val clientIdOpt = Option(header.client_id) map { _.name }
      val contextBuf = ArrayBuffer.empty[(ChannelBuffer, ChannelBuffer)]
      contextBuf += TraceContext.newKVTuple(traceId)
      contextBuf += ClientIdContext.newKVTuple(clientIdOpt)
      if (header.contexts != null) {
        val iter = header.contexts.iterator()
        while (iter.hasNext) {
          contextBuf += contextStructToKVTuple(iter.next())
        }
      }

      Message.Tdispatch(
        Message.MinTag, contextBuf.toSeq, "", Dtab.empty, ChannelBuffers.wrappedBuffer(request_))
    }

    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent)  {
      val buf = e.getMessage.asInstanceOf[ChannelBuffer]
      super.messageReceived(ctx, new UpstreamMessageEvent(
        e.getChannel, Message.encode(thriftToMux(buf)), e.getRemoteAddress))
    }

    override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) {
      Message.decode(e.getMessage.asInstanceOf[ChannelBuffer]) match {
        case Message.RdispatchOk(_, _, rep) =>
          super.writeRequested(ctx,
            new DownstreamMessageEvent(e.getChannel, e.getFuture,
              ChannelBuffers.wrappedBuffer(responseHeader, rep), e.getRemoteAddress))

        // Non-mux Clients can't handle T-type control messages, so we simulate responses.
        case Message.Tdrain(tag) =>
          e.getFuture.setSuccess()
          super.messageReceived(ctx,
            new UpstreamMessageEvent(
              e.getChannel,
              Message.encode(Message.Rdrain(tag)),
              e.getRemoteAddress))

        case Message.Tping(tag) =>
          e.getFuture.setSuccess()
          super.messageReceived(ctx,
            new UpstreamMessageEvent(
              e.getChannel,
              Message.encode(Message.Rping(tag)),
              e.getRemoteAddress))

        case Message.ControlMessage(tag) =>
          e.getFuture.setSuccess()
          super.messageReceived(ctx,
            new UpstreamMessageEvent(
              e.getChannel,
              Message.encode(
                Message.Rerr(tag, "Unable to send Mux control message to non-Mux client")
              ),
              e.getRemoteAddress))

        case Message.RdispatchError(_, _, error) =>
          // OK to throw an exception here as ServerBridge take cares it
          // by logging the error and then closing the channel.
          throw UnexpectedRequestException(error)

        case unexpected =>
          throw UnexpectedRequestException(
            "Unexpected request type %s".format(unexpected.getClass.getName))
      }
    }
  }

  private class TFramedToMux extends SimpleChannelHandler {
    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent)  {
      val buf = e.getMessage.asInstanceOf[ChannelBuffer]
      super.messageReceived(ctx,
        new UpstreamMessageEvent(
          e.getChannel,
          Message.encode(Message.Tdispatch(Message.MinTag, Seq.empty, "", Dtab.empty, buf)),
          e.getRemoteAddress))
    }

    override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) {
      Message.decode(e.getMessage.asInstanceOf[ChannelBuffer]) match {
        case Message.RdispatchOk(_, _, rep) =>
          super.writeRequested(ctx,
            new DownstreamMessageEvent(e.getChannel, e.getFuture, rep, e.getRemoteAddress))

        // Non-mux Clients can't handle T-type control messages, so we simulate responses.
        case Message.Tdrain(tag) =>
          e.getFuture.setSuccess()
          super.messageReceived(ctx,
            new UpstreamMessageEvent(
              e.getChannel,
              Message.encode(Message.Rdrain(tag)),
              e.getRemoteAddress))

        case Message.Tping(tag) =>
          e.getFuture.setSuccess()
          super.messageReceived(ctx,
            new UpstreamMessageEvent(
              e.getChannel,
              Message.encode(Message.Rping(tag)),
              e.getRemoteAddress))

        case Message.ControlMessage(tag) =>
          e.getFuture.setSuccess()
          super.messageReceived(ctx,
            new UpstreamMessageEvent(
              e.getChannel,
              Message.encode(
                Message.Rerr(tag, "Unable to send Mux control message to non-Mux client")
              ),
              e.getRemoteAddress))

        case Message.RdispatchError(_, _, error) =>
          // OK to throw an exception here as ServerBridge take cares it
          // by logging the error and then closing the channel.
          throw UnexpectedRequestException(error)

        case unexpected =>
          throw UnexpectedRequestException(
            "Unexpected request type %s".format(unexpected.getClass.getName))
      }
    }
  }

  class RequestSerializer(pendingReqs: Int = 0) extends SimpleChannelHandler {
    // Note: Since there can only be at most one pending request at any time,
    // the only race condition that needs to be handled is one thread (a
    // Netty worker thread) executes messageReceived while another thread
    // executes writeRequested (the thread satisfies the request)
    private[this] val q = new LinkedBlockingDeque[MessageEvent]
    private[this] val n = new AtomicInteger(pendingReqs)

    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      if (n.incrementAndGet() > 1) q.offer(e)
      else super.messageReceived(ctx, e)
    }

    override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) {
      super.writeRequested(ctx, e)
      if (n.decrementAndGet() > 0) {
        // Need to call q.take() Since incrementing n and enqueueing the
        // request are not atomic. n>0 guarantees q.take() does not block forever.
        super.messageReceived(ctx, q.take())
      }
    }
  }

  private object Upgrader {
    val upNegotiationAck = {
      val buffer = new OutputBuffer(protocolFactory)
      buffer().writeMessageBegin(
        new TMessage(ThriftTracing.CanTraceMethodName, TMessageType.REPLY, 0))
      val upgradeReply = new UpgradeReply
      upgradeReply.write(buffer())
      buffer().writeMessageEnd()
      ChannelBuffers.copiedBuffer(buffer.toArray)
    }
  }

  private class Upgrader extends SimpleChannelHandler {
    import Upgrader._

    private[this] def isTTwitterUpNegotiation(req: ChannelBuffer): Boolean = {
      try {
        val buffer = new InputBuffer(ThriftMuxUtil.bufferToArray(req), protocolFactory)
        val msg = buffer().readMessageBegin()
        msg.`type` == TMessageType.CALL &&
          msg.name == ThriftTracing.CanTraceMethodName
      } catch {
        case NonFatal(_) => false
      }
    }

    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      val buf = e.getMessage.asInstanceOf[ChannelBuffer]
      Try { Message.decode(buf.duplicate()) } match {
        // We assume that a bad message decode indicates an old-style
        // session. Due to Mux message numbering, a binary-encoded
        // thrift frame corresponds to an Rerr message with tag
        // 65537. Note that in this context, an R-message is never
        // valid.
        //
        // Binary-encoded thrift messages have the format
        //
        //     header:4 n:4 method:n seqid:4
        //
        // The header is
        //
        //     0x80010000 | type
        //
        // where the type of CALL is 1; the type of ONEWAY is 4. This makes 
        // the first four bytes of a CALL message 0x80010001.
        //
        // Mux messages begin with
        //
        //     Type:1 tag:3
        //
        // Rerr is type 0x80, so we see the above thrift header
        // Rerr corresponds to (tag=0x010001).
        //
        // The hazards of protocol multiplexing.
        case Throw(_: BadMessageException) | Return(Message.Rerr(65537, _)) | Return(Message.Rerr(65540, _))=>
          // Add a ChannelHandler to serialize the requests since we may
          // deal with a client that pipelines requests
          ctx.getPipeline.addBefore(ctx.getName, "request_serializer", new RequestSerializer(1))
          if (isTTwitterUpNegotiation(buf)) {
            ctx.getPipeline.replace(this, "twitter_thrift_to_mux", new TTwitterToMux)
            Channels.write(ctx, e.getFuture, upNegotiationAck, e.getRemoteAddress)
          } else {
            ctx.getPipeline.replace(this, "framed_thrift_to_mux", new TFramedToMux)
            super.messageReceived(ctx,
              new UpstreamMessageEvent(
                e.getChannel,
                Message.encode(Message.Tdispatch(Message.MinTag, Seq.empty, "", Dtab.empty, buf)),
                e.getRemoteAddress))
          }

        case Return(_) =>
          ctx.getPipeline.remove(this)
          super.messageReceived(ctx, e)

        case Throw(exc) => throw exc
      }
    }
  }

  def getPipeline() = {
    val pipeline = mux.PipelineFactory.getPipeline()
    pipeline.addLast("upgrader", new Upgrader)
    pipeline
  }
}

private[finagle] object PipelineFactory extends PipelineFactory(Protocols.binaryFactory())
