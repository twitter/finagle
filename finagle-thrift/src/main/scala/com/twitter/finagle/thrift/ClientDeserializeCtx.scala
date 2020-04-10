package com.twitter.finagle.thrift

import com.twitter.finagle.context.Contexts
import com.twitter.finagle.service.ReqRep
import com.twitter.util.Try

/**
 * Used by Thrift and ThriftMux Client to facilitate giving
 * the Finagle stack access to various data that are computed
 * outside of Finagle's stack.
 *
 * This includes:
 *  - the deserialized forms of Thrift requests and responses.
 *  - the name of the rpc.
 *  - the elapsed time to serialize the request.
 *  - the elapsed time to deserialize the response.
 *
 * While this is thread-safe, it should only be used for the life
 * of a single request/response pair.
 *
 * When using [[https://twitter.github.io/scrooge/ Scrooge]] for code
 * generation, a proper `ClientDeserializeCtx` will be available
 * to code via `Contexts.local(ClientDeserializeCtx.Key)`.
 *
 * @note this class has evolved and it's name is now a bit too specific
 *       for its more expanded role.
 */
class ClientDeserializeCtx[Rep](val request: Any, replyDeserializer: Array[Byte] => Try[Rep])
    extends (Array[Byte] => ReqRep) {

  // Thread safety provided via synchronization on `this`.
  //   Note that coarse grained synchronization suffices here as
  //   there should be no contention between threads, though reads
  //   and writes are likely to happen on different threads.
  private[this] var deserialized: Try[Rep] = null
  private[this] var _rpcName: Option[String] = None
  // `Durations` are not used for these, to avoid object allocations.
  private[this] var serializationNanos: Long = Long.MinValue
  private[this] var deserializationNanos: Long = Long.MinValue

  /**
   * Deserialize the given bytes.
   *
   * Ensures that deserialization will only happen once for non fan-out responses
   * regardless of future inputs. If different bytes are seen on future calls,
   * this will still return the first deserialized result.
   */
  def deserialize(responseBytes: Array[Byte]): Try[Rep] = synchronized {
    if (deserialized == null) {
      val start = System.nanoTime
      deserialized = replyDeserializer(responseBytes)
      deserializationNanos = System.nanoTime - start
    }
    deserialized
  }

  def apply(responseBytes: Array[Byte]): ReqRep = {
    ReqRep(request, deserialize(responseBytes))
  }

  /**
   * Sets the name of the the rpc method.
   */
  def rpcName(name: String): Unit = {
    val asOption = Option(name)
    synchronized {
      _rpcName = asOption
    }
  }

  /**
   * Gets the rpc method's name, if set.
   */
  def rpcName: Option[String] = synchronized {
    _rpcName
  }

  /**
   * Applies deserializer to one response from bathed responses
   * @note: This won't update the `deserialized` content
   */
  private[finagle] def deserializeFromBatched(responseBytes: Array[Byte]): Try[Rep] = synchronized {
    replyDeserializer(responseBytes)
  }

  /**
   * Sets the merged deserialized response
   */
  private[finagle] def mergedDeserializedResponse(response: Try[Any]): Unit = synchronized {
    deserialized = response.asInstanceOf[Try[Rep]]
  }

  /**
   * Sets how long, in nanoseconds, it took for the client to
   * serialize the request into Thrift format.
   */
  def serializationTime(nanos: Long): Unit = synchronized {
    serializationNanos = nanos
  }

  /**
   * Gets how long, in nanoseconds, it took for the client to
   * serialize the request into Thrift format.
   *
   * Negative values indicate that this was not recorded and
   * as such, should be ignored.
   */
  def serializationTime: Long = synchronized {
    serializationNanos
  }

  /**
   * Gets how long, in nanoseconds, it took for the client to
   * [[deserialize]] the response from Thrift format.
   *
   * Negative values indicate that this was not recorded and
   * as such, should be ignored.
   */
  def deserializationTime: Long = synchronized {
    deserializationNanos
  }

}

object ClientDeserializeCtx {

  val Key: Contexts.local.Key[ClientDeserializeCtx[_]] =
    new Contexts.local.Key[ClientDeserializeCtx[_]]

  private val NullDeserializeFn: () => ClientDeserializeCtx[Nothing] = () => nullDeserializeCtx

  def get: ClientDeserializeCtx[_] = Contexts.local.getOrElse(Key, NullDeserializeFn)

  val nullDeserializeCtx: ClientDeserializeCtx[Nothing] =
    new ClientDeserializeCtx[Nothing](null, null) {
      override def rpcName(name: String): Unit = ()
      override def serializationTime(nanos: Long): Unit = ()
    }
}
