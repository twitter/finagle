package com.twitter.finagle.stream

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.frame.{LengthFieldBasedFrameDecoder, LengthFieldPrepender}

import com.twitter.concurrent.{Channel, ChannelSource, Observer}
import com.twitter.finagle.{
  Codec, CodecFactory, Service, Filter}
import com.twitter.finagle.util.Conversions._
import com.twitter.util.{Future, Promise, Return}

/**
 * A DuplexStreamCodec exposes two `Channel[ChannelBuffer]` (inbound and outbound).
 * The DuplexStreamCodec will acknowledge sends on the outbound channel when netty
 * acknowledges the write. Optionally, you can pass to the codec constructor
 * and sends will only be acknowledged when the corresponding inbound channel on the
 * receiving end acknowledges its send.
 *
 * The channels are exposed via Finagle's Service interface:
 *  val factory = ClientBuilder()
 *    .codec(new DuplexStreamCodec(true))
 *    .hosts(Seq(address))
 *    .buildFactory()
 *  val service = factory.make()
 *  service.foreach { client =>
 *    val outbound = new ChannelSource[ChannelBuffer]
 *    val inbound: Future[Channel[ChannelBuffer]] = client(outbound)
 *  }
 */

class FramingCodec extends SimpleChannelHandler {
  private[this] val decoder = new LengthFieldBasedFrameDecoder(0x7FFFFFFF, 0, 4, 0, 4)
  private[this] val encoder = new LengthFieldPrepender(4)

  override def handleUpstream(ctx: ChannelHandlerContext, e: ChannelEvent) =
    decoder.handleUpstream(ctx, e)

  override def handleDownstream(ctx: ChannelHandlerContext, e: ChannelEvent) =
    encoder.handleDownstream(ctx, e)
}

private[stream] abstract class BufferToChannelCodec extends SimpleChannelHandler {
  private[this] val inbound: Promise[ChannelSource[ChannelBuffer]] = new Promise[ChannelSource[ChannelBuffer]]
  private[this] var outbound: Channel[ChannelBuffer] = null
  private[this] var outboundObserver: Observer = null

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    if (inbound.isDefined && inbound().isOpen) {
      inbound().close()
    }

    if (outboundObserver != null) {
     outboundObserver.dispose()
    }
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) = {
    e.getMessage match {
      case buf: ChannelBuffer => inbound() send buf
      case _ => throw new Exception("Unexpected message received")
    }
  }

  override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    require(!inbound.isDefined, "Inbound channel already created.")

    // We don't want message to go to the ether
    ctx.getChannel.setReadable(false)

    inbound() = Return(new ChannelSource[ChannelBuffer])
  }

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) = {
    e.getMessage match {
      case channel: Channel[ChannelBuffer] =>
        synchronized {
          require(outbound == null, "Already have an outbound channel")
          outbound = channel
        }
      case _ => throw new Exception("Unexpected message written")
    }

    outboundObserver = outbound respond { buf =>
      val messageFuture = Channels.future(ctx.getChannel)
      val result = messageFuture.toTwitterFuture
      Channels.write(ctx, messageFuture, buf)
      result
    }

    outbound.closes onSuccess { _ =>
      val messageFuture = Channels.future(ctx.getChannel)
      val result = messageFuture.toTwitterFuture
      Channels.close(ctx, messageFuture)
      result
    }
  }

  protected[this] def sendInboundUpstream(ctx: ChannelHandlerContext, e: ChannelEvent) {
    inbound foreach { ch =>
      Channels.fireMessageReceived(ctx, ch)
      ctx.getChannel.setReadable(true)
    }
  }
}

class ServerBufferToChannelCodec extends BufferToChannelCodec {
  override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    super.channelConnected(ctx, e)
    sendInboundUpstream(ctx, e)
  }
}

class ClientBufferToChannelCodec extends BufferToChannelCodec {
  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) = {
    super.writeRequested(ctx, e)
    sendInboundUpstream(ctx, e)
  }
}

class DuplexStreamCodec(useReliable: Boolean)
  extends CodecFactory[Channel[ChannelBuffer], Channel[ChannelBuffer]]
{
  def server = Function.const {
    new Codec[Channel[ChannelBuffer], Channel[ChannelBuffer]] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("framingCodec", new FramingCodec)
          pipeline.addLast("channelify", new ServerBufferToChannelCodec)
          pipeline
        }
      }

      override def prepareService(service: Service[Channel[ChannelBuffer], Channel[ChannelBuffer]]) = {
        if (useReliable) {
          Future.value(new ReliableDuplexServerFilter().andThen(service))
        } else {
          Future.value(service)
        }
      }
    }
  }

  def client = Function.const {
    new Codec[Channel[ChannelBuffer], Channel[ChannelBuffer]] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("framingCodec", new FramingCodec)
          pipeline.addLast("channelify", new ClientBufferToChannelCodec)
          pipeline
        }
      }

      override def prepareService(service: Service[Channel[ChannelBuffer], Channel[ChannelBuffer]]) = {
        if (useReliable) {
          Future.value(new ReliableDuplexClientFilter().andThen(service))
        } else {
          Future.value(service)
        }
      }
    }
  }
}
