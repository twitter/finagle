package com.twitter.finagle.stream

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.frame.{LengthFieldBasedFrameDecoder, LengthFieldPrepender}

import com.twitter.concurrent.{Broker, Offer}
import com.twitter.finagle.{
  Codec, CodecFactory, ServerCodecConfig, ClientCodecConfig}
import com.twitter.util.{Future, Promise, Return}

/**
 * DuplexStreamCodec allows you to send and receive messages to server running
 * the same codec. It sends to you a DuplexStreamHandle that contains:
 *  - messages: Offer[ChannelBuffer]
 *  - onClose: Future[Unit]
 *  - close: () => Unit
 * and it expects you to send/return to it a Offer[ChannelBuffer] for outbound
 * messages. This codec synchronizes immediately on outbound messages, and does
 * not provide you with information about the other side's offer synchronization.
 * example:
 * val factory = ClientBuilder()
 *   .codec(DuplexStreamCodec())
 *   .hosts(Seq(address))
 *   .buildFactory()
 * val service =
 * factory() onSuccess { client =>
 *   val outbound = new Broker[ChannelBuffer]
 *   val handle: Future[DuplexStreamHandle] = client(outbound.recv)
 *   handle.messages foreach { inboundMessage => ... }
 */

case class DuplexStreamHandle(
  messages: Offer[ChannelBuffer],
  onClose: Future[Unit],
  close: () => Unit
)

class FramingCodec extends SimpleChannelHandler {
  private[this] val decoder = new LengthFieldBasedFrameDecoder(0x7FFFFFFF, 0, 4, 0, 4)
  private[this] val encoder = new LengthFieldPrepender(4)

  override def handleUpstream(ctx: ChannelHandlerContext, e: ChannelEvent) =
    decoder.handleUpstream(ctx, e)

  override def handleDownstream(ctx: ChannelHandlerContext, e: ChannelEvent) =
    encoder.handleDownstream(ctx, e)
}

private[stream] abstract class BufferToChannelCodec extends SimpleChannelHandler {
  private[this] val inbound = new Broker[ChannelBuffer]
  private[this] val onClose = new Promise[Unit]

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    onClose() = Return(())
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) = {
    e.getMessage match {
      case buf: ChannelBuffer =>
        inbound ! buf
      case m =>
        val msg = "Unexpected message type sent upstream: %s".format(m.getClass.toString)
        throw new IllegalArgumentException(msg)
    }
  }

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) = {
    e.getMessage match {
      case outbound: Offer[_] =>
        e.getFuture.setSuccess()
        outbound foreach { message =>
          Channels.write(ctx, Channels.future(ctx.getChannel), message)
        }
      case m =>
        val msg = "Unexpected message type sent downstream: %s".format(m.getClass.toString)
        throw new IllegalArgumentException(msg)
    }
  }

  protected[this] def sendHandleUpstream(ctx: ChannelHandlerContext) {
    def close() { Channels.close(ctx.getChannel) }

    val streamHandle = DuplexStreamHandle(inbound.recv, onClose, close)
    Channels.fireMessageReceived(ctx, streamHandle)
  }
}

class ServerBufferToChannelCodec extends BufferToChannelCodec {
  override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    super.channelConnected(ctx, e)
    sendHandleUpstream(ctx)
  }
}

class ClientBufferToChannelCodec extends BufferToChannelCodec {
  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) {
    super.writeRequested(ctx, e)
    sendHandleUpstream(ctx)
  }
}

class DuplexStreamServerCodec extends Codec[DuplexStreamHandle, Offer[ChannelBuffer]] {
  def pipelineFactory = new ChannelPipelineFactory {
    def getPipeline = {
      val pipeline = Channels.pipeline()
      pipeline.addLast("framer", new FramingCodec)
      pipeline.addLast("upgrade", new ServerBufferToChannelCodec)
      pipeline
    }
  }
}

class DuplexStreamClientCodec extends Codec[Offer[ChannelBuffer], DuplexStreamHandle] {
  def pipelineFactory = new ChannelPipelineFactory {
    def getPipeline = {
      val pipeline = Channels.pipeline()
      pipeline.addLast("framer", new FramingCodec)
      pipeline.addLast("upgrade", new ClientBufferToChannelCodec)
      pipeline
    }
  }
}

class DuplexStreamServerCodecFactory
  extends CodecFactory[DuplexStreamHandle, Offer[ChannelBuffer]]#Server
{
  def apply(config: ServerCodecConfig) = new DuplexStreamServerCodec()
}

class DuplexStreamClientCodecFactory
  extends CodecFactory[Offer[ChannelBuffer], DuplexStreamHandle]#Client
{
  def apply(config: ClientCodecConfig) = new DuplexStreamClientCodec()
}

object DuplexStreamServerCodec {
  def apply() = new DuplexStreamServerCodecFactory
  def get() = apply()
}

object DuplexStreamClientCodec {
  def apply() = new DuplexStreamClientCodecFactory
  def get() = apply()
}
