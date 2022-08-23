package com.twitter.finagle.thrift

import com.twitter.finagle.context.Contexts
import com.twitter.io.Buf
import com.twitter.util.Return
import com.twitter.util.Try

case class ClientId(name: String) {

  /**
   * Executes the given function with this ClientId set as the current
   * ClientId.  The current ClientId before executing this will be restored
   * on completion.
   */
  def asCurrent[T](f: => T): T = ClientId.let(Some(this))(f)

  /**
   * USE WITH CARE:
   *
   * This would force-override the ClientId in the RPC context. This can be used to set a
   * per-request ClientId, which could be helpful in the multi-tenant systems.
   */
  private[twitter] def asOverride[T](f: => T): T = ClientId.letOverride(this)(f)
}

/**
 * `ClientId` provides the client identification of the incoming request if available.
 * It is set at the beginning of the request and is available throughout the life-cycle
 * of the request. It is iff the client has an upgraded finagle connection and has chosen
 * to specify the client ID in their codec.
 */
object ClientId {

  private val OverriddenClientId = new Contexts.local.Key[ClientId]

  // As a matter of legacy, we need to support the notion of
  // an empty client id. Old version of contexts could serialize
  // the absence of a client id with an empty buffer.
  private[finagle] val clientIdCtx =
    new Contexts.broadcast.Key[Option[ClientId]]("com.twitter.finagle.thrift.ClientIdContext") {
      def marshal(clientId: Option[ClientId]): Buf = clientId match {
        case None => Buf.Empty
        case Some(ClientId(name)) => Buf.Utf8(name)
      }

      def tryUnmarshal(buf: Buf): Try[Option[ClientId]] = buf match {
        case b if b.isEmpty => Return.None
        case Buf.Utf8(name) => Return(Some(ClientId(name)))
      }
    }

  private[this] val NoClientFn: () => Option[ClientId] = () => None

  def current: Option[ClientId] =
    Contexts.broadcast.getOrElse(clientIdCtx, NoClientFn)

  def overridden: Option[ClientId] = Contexts.local.get(OverriddenClientId)

  /**
   * See [[ClientId.asCurrent]]
   */
  private[finagle] def let[R](clientId: Option[ClientId])(f: => R): R = {
    clientId match {
      case Some(_) => Contexts.broadcast.let(clientIdCtx, clientId)(f)
      case None => Contexts.broadcast.letClear(clientIdCtx)(f)
    }
  }

  private def letOverride[R](clientId: ClientId)(f: => R): R =
    Contexts.local.let(OverriddenClientId, clientId)(f)
}
