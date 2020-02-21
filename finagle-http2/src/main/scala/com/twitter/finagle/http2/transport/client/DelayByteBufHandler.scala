package com.twitter.finagle.http2.transport.client
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ByteToMessageDecoder

/**
 * DelayByteBufHandler holds the inbound ByteBuf until itself is removed.
 * ByteToMessageDecoder accumulates the unread bytes in an internal buffer,
 * it releases the internal buffer and emits the bytes to the pipeline on
 * removal.
 *
 * @note: This is a temporary fix of CSL-9372, which reports a race condition:
 *       some client inbound SETTINGS frames got lost during the SSL handshake.
 *       This handler holds the unread bytes until the handshake completed.
 *       (com.twitter.finagle.netty4.ssl.client.SslClientVerificationHandler#handshakeComplete())
 *       We hope to remove this after avoiding that race.
 */
private class DelayByteBufHandler extends ByteToMessageDecoder {
  override def decode(
    channelHandlerContext: ChannelHandlerContext,
    byteBuf: ByteBuf,
    list: java.util.List[AnyRef]
  ): Unit = {}
}
