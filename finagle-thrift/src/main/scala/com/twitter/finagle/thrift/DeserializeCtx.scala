package com.twitter.finagle.thrift

import com.twitter.finagle.context.Contexts
import com.twitter.util.Try

@deprecated("Use ClientDeserializeCtx.", "2018-8-13")
final class DeserializeCtx[Rep](
  override val request: Any,
  replyDeserializer: Array[Byte] => Try[Rep])
    extends ClientDeserializeCtx[Rep](request, replyDeserializer)

@deprecated("Use ClientDeserializeCtx.", "2018-8-13")
object DeserializeCtx {
  val Key: Contexts.local.Key[ClientDeserializeCtx[_]] = ClientDeserializeCtx.Key
}
