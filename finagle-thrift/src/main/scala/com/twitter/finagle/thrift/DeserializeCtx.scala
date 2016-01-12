package com.twitter.finagle.thrift

import com.twitter.finagle.context.Contexts
import com.twitter.scrooge.ThriftStruct
import com.twitter.util.Try

/**
 * Used by Thrift and ThriftMux to facilitate giving
 * the Finagle stack access to the deserialized forms of
 * Thrift requests and responses.
 *
 * When using [[http://twitter.github.io/scrooge/ Scrooge]] for code
 * generation, a proper `DeserializationCtx` will be available
 * to code via `Contexts.local(DeserializeCtx.Key)`.
 *
 * While this is thread-safe, it should only be used for the life
 * of a single request/response pair.
 *
 * @param request the request that was used to generate response
 * to be seen by [[deserialize]].
 */
class DeserializeCtx[Rep](
    val request: ThriftStruct,
    replyDeserializer: Array[Byte] => Try[Rep]) {

  // thread safety provided via synchronization on this
  private var deserialized: Try[Rep] = null

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
}

object DeserializeCtx {

  val Key: Contexts.local.Key[DeserializeCtx[_]] =
    new Contexts.local.Key[DeserializeCtx[_]]

}
