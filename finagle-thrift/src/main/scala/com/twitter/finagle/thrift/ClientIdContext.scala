package com.twitter.finagle.thrift

import com.twitter.finagle.{Context, ContextHandler}
import com.twitter.io.Buf
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}

private[finagle] object ClientIdContext {
  val Key = Buf.Utf8("com.twitter.finagle.thrift.ClientIdContext")
  val KeyBytes = Context.keyBytes(Key)
  val KeyBytesChannelBuffer = ChannelBuffers.wrappedBuffer(KeyBytes)

  /**
   * Serialize an `Option[String]` representing an optional ClientId name into
   * a tuple of key->value ChannelBuffers. Useful for piecing together context
   * pairs to give to the construct of `Tdispatch`.
   */
  private[finagle] def newKVTuple(clientIdOpt: Option[String]): (ChannelBuffer, ChannelBuffer) = {
    val clientIdBuf = clientIdOpt match {
      case Some(clientId) =>
        val vBuf = Buf.Utf8(clientId)
        val bytes = new Array[Byte](vBuf.length)
        vBuf.write(bytes, 0)
        ChannelBuffers.wrappedBuffer(bytes)

      case None => ChannelBuffers.EMPTY_BUFFER
    }

    KeyBytesChannelBuffer.duplicate() -> clientIdBuf
  }
}

/**
 * A context handler for ClientIds.
 */
private[finagle] class ClientIdContext extends ContextHandler {
  val key = ClientIdContext.Key

  def handle(body: Buf) {
    body match {
      case buf if buf.length == 0 => ClientId.clear()
      case Buf.Utf8(name) => ClientId.set(Some(ClientId(name)))
      case invalid => ClientId.clear()
    }
  }

  // It arguably doesn't make sense to pass through ClientIds, since
  // they are meant to identify the immediate client of a downstream
  // system.
  def emit(): Option[Buf] = ClientId.current map { id => Buf.Utf8(id.name) }
}
