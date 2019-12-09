package com.twitter.finagle.http.codec.context

import com.twitter.io.Buf
import com.twitter.util.{Base64StringEncoder, Try}

/**
 * LoadableHttpContext is picked up via [[com.twitter.finagle.util.LoadService]] providing
 * a way to marshal custom [[com.twitter.finagle.context.Context]] types into http headers
 * using [[HttpContext]].
 *
 * LoadableHttpContext will by default use [[com.twitter.finagle.context.Contexts.broadcast.Key]]'s
 * `marshal`, `tryUnmarshal` and `id` to create the header (key: String, value: String) pairs. The
 * result of `marshal` is base64 encoded. Exceptions thrown from `tryUnmarshal` or `fromHeader` will
 * be suppressed and only the faulty key name will be logged (debug level).
 *
 *
 * Override the methods of [[HttpContext]] to define a custom scheme. All headers are prefixed with
 * "Finagle-Ctx-".
 */
private[twitter] abstract class LoadableHttpContext extends HttpContext {
  def toHeader(value: ContextKeyType): String = {
    Base64StringEncoder.encode(Buf.ByteArray.Owned.extract(key.marshal(value)))
  }

  def fromHeader(header: String): Try[ContextKeyType] = {
    key.tryUnmarshal(Buf.ByteArray.Owned(Base64StringEncoder.decode(header)))
  }
}
