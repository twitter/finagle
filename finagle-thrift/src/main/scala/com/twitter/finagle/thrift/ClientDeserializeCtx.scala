package com.twitter.finagle.thrift

import com.twitter.finagle.context.Contexts
import com.twitter.finagle.service.ReqRep
import com.twitter.util.Try

/**
 * Used by Thrift and ThriftMux Client to facilitate giving
 * the Finagle stack access to the deserialized forms of
 * Thrift requests and responses.
 *
 * While this is thread-safe, it should only be used for the life
 * of a single request/response pair.
 *
 * When using [[https://twitter.github.io/scrooge/ Scrooge]] for code
 * generation, a proper `ClientDeserializeCtx` will be available
 * to code via `Contexts.local(ClientDeserializeCtx.Key)`.
 */
class ClientDeserializeCtx[Rep](val request: Any, replyDeserializer: Array[Byte] => Try[Rep])
  extends (Array[Byte] => ReqRep) {

  // thread safety provided via synchronization on this
  private[this] var deserialized: Try[Rep] = null

  /**
   * Deserialize the given bytes.
   *
   * Ensures that deserialization will only happen once regardless of future
   * inputs. If different bytes are seen on future calls, this will still
   * return the first deserialized result.
   */
  def deserialize(responseBytes: Array[Byte]): Try[Rep] = synchronized {
    if (deserialized == null)
      deserialized = replyDeserializer(responseBytes)
    deserialized
  }

  def apply(responseBytes: Array[Byte]): ReqRep = {
    ReqRep(request, deserialize(responseBytes))
  }
}

object ClientDeserializeCtx {

  val Key: Contexts.local.Key[ClientDeserializeCtx[_]] =
    new Contexts.local.Key[ClientDeserializeCtx[_]]

  private val NullDeserializeFn: () => ClientDeserializeCtx[Nothing] = () => nullDeserializeCtx

  def get: ClientDeserializeCtx[_] = Contexts.local.getOrElse(Key, NullDeserializeFn)

  val nullDeserializeCtx: ClientDeserializeCtx[Nothing] =
    new ClientDeserializeCtx[Nothing](null, null)
}
