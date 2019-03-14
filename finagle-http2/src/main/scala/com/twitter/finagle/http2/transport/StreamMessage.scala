package com.twitter.finagle.http2.transport

import io.netty.handler.codec.http.HttpObject
import io.netty.handler.codec.http2.Http2Exception.HeaderListSizeException

/**
 * Represents objects emitted in the HTTP/2 transports. These are used to convert between
 * HTTP/2 and HTTP/1.1 representations.
 */
private[http2] sealed trait StreamMessage

private[http2] object StreamMessage {

  final case class Message(obj: HttpObject, streamId: Int) extends StreamMessage
  final case class GoAway(obj: HttpObject, lastStreamId: Int, errorCode: Long) extends StreamMessage
  final case class Rst(streamId: Int, errorCode: Long) extends StreamMessage
  // exn is purposefully narrow for now, we can expand it if necessary
  final case class StreamException(exn: HeaderListSizeException, streamId: Int)
      extends StreamMessage

  case object Ping extends StreamMessage
}
