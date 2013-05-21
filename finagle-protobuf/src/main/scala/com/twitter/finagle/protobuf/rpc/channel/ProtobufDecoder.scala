package com.twitter.finagle.protobuf.rpc.channel


/**
 *
 * Knows how to decode a ("doSomething()", ProtobufMessage) binary message.
 *
 * Message Format
 * ==============
 *
 * Offset: 0             4                8
 *         +-------------+----------------+------------------+
 *         | method code | message length | protobuf message |
 *         +-------------+----------------+------------------+
 *
 */

trait ProtobufDecoder {

  def decode(ctx: ChannelHandlerContext, channel: Channel, buf: ChannelBuffer, repo: MethodLookup): Object = {

    if (buf.readableBytes() < 8) {
      return null
    }

    buf.markReaderIndex()

    val methodCode = buf.readInt()
    val msgLen = buf.readInt()

    // do we have enough bytes to decode the message?
    if (buf.readableBytes() < msgLen) {
      buf.resetReaderIndex();
      return null;
    }

    val methodName = repo.lookup(methodCode)
    val prototype = getPrototype(methodName)

    val msgBuf = buf.readBytes(msgLen);
    def message = prototype.newBuilderForType().mergeFrom(
      new ChannelBufferInputStream(msgBuf)).build()

    (methodName, message)
  }

  def getPrototype(methodName: String): Message;

}
