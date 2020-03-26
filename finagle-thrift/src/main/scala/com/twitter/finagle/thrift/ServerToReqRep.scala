package com.twitter.finagle.thrift

import com.twitter.finagle.context.Contexts
import com.twitter.finagle.service.ReqRep

/**
 * Used by Thrift and ThriftMux Server to facilitate giving
 * the Finagle stack access to the deserialized forms of
 * Thrift requests and responses.
 *
 * While this is thread-safe, it should only be used for the life
 * of a single request/response pair.
 *
 * When using [[https://twitter.github.io/scrooge/ Scrooge]] for code
 * generation, a proper `ServerToReqRep` will be available
 * to code via `Contexts.local(ServerToReqRep.Key)`.
 */
final class ServerToReqRep extends (Array[Byte] => ReqRep) {

  // thread safety provided via synchronization on this
  private[this] var _reqRep: Option[ReqRep] = None

  /**
   * Set the ReqRep in deserialized form, _reqRep can only be set once
   * (only the first time is valid for a thread).
   */
  def setReqRep(reqRep: ReqRep): Unit = synchronized {
    if (_reqRep.isEmpty) _reqRep = Some(reqRep)
  }

  /**
   * Returns the ReqRep set before, `null` if setReqRep hasn't been called.
   */
  def apply(responseBytes: Array[Byte]): ReqRep = synchronized {
    _reqRep match {
      case Some(r) => r
      case None => null
    }
  }
}

object ServerToReqRep {

  val Key: Contexts.local.Key[ServerToReqRep] = new Contexts.local.Key[ServerToReqRep]

  private val NullDeserializeFn: () => ServerToReqRep = () => nullDeserializeCtx

  def get: ServerToReqRep = Contexts.local.getOrElse(Key, NullDeserializeFn)

  val nullDeserializeCtx: ServerToReqRep = new ServerToReqRep

  def setCtx(reqRep: ReqRep): Unit = {
    val ctx = get
    if (ctx ne nullDeserializeCtx) ctx.setReqRep(reqRep)
  }
}
