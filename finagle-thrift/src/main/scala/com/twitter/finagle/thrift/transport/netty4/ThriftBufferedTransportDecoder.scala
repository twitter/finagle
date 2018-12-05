package com.twitter.finagle.thrift.transport.netty4

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ReplayingDecoder
import java.util
import org.apache.thrift.protocol.{TProtocolFactory, TProtocolUtil, TType}
import org.apache.thrift.transport.{TTransport, TTransportException}

/**
 * Frames Thrift messages by attempting to fast-forward through the message stream
 * and measuring the number of bytes that composed the message
 */
private[netty4] class ThriftBufferedTransportDecoder(protocolFactory: TProtocolFactory)
    extends ReplayingDecoder[java.lang.Void] {

  /**
   * Adapts a single Netty ByteBuf to a Thrift TTransport
   */
  private class ByteBufToTransport(underlying: ByteBuf) extends TTransport {
    override def isOpen: Boolean = true
    override def open(): Unit = {}
    override def close(): Unit = {}

    override def read(buffer: Array[Byte], offset: Int, length: Int): Int = {
      val bytesToRead = math.min(length, underlying.readableBytes)
      underlying.readBytes(buffer, offset, bytesToRead)
      bytesToRead
    }

    override def write(buffer: Array[Byte], offset: Int, length: Int): Unit = {
      underlying.writeBytes(buffer, offset, length)
    }
  }

  override def decode(ctx: ChannelHandlerContext, buffer: ByteBuf, out: util.List[AnyRef]): Unit = {
    val transport = new ByteBufToTransport(buffer)
    val iprot = protocolFactory.getProtocol(transport)

    val beginIndex = buffer.readerIndex
    buffer.markReaderIndex()

    iprot.readMessageBegin()
    TProtocolUtil.skip(iprot, TType.STRUCT)
    iprot.readMessageEnd()

    val endIndex = buffer.readerIndex
    buffer.resetReaderIndex()

    val message = buffer.readSlice(endIndex - beginIndex)

    // Retain the buffer: messages created by `readSlice` are not automatically retained
    // and the sliced message will belong to the next stage
    message.retain()
    out.add(message)
  }

  override def decodeLast(ctx: ChannelHandlerContext, in: ByteBuf, out: util.List[AnyRef]): Unit = {
    try decode(ctx, in, out)
    catch { case _: TTransportException => /* NOOP */ }
  }
}
