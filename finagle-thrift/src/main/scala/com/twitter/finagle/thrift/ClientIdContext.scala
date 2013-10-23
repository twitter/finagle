package com.twitter.finagle.thrift

import com.twitter.finagle.{Context, ContextHandler}
import com.twitter.io.Buf

private[finagle] object ClientIdContext {
  val Key = Buf.Utf8("com.twitter.finagle.thrift.ClientIdContext")
  val KeyBytes = Context.keyBytes(Key)
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

  def emit(): Option[Buf] = ClientId.current map { id => Buf.Utf8(id.name) }
}
