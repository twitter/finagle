package com.twitter.finagle.param

import com.twitter.finagle.Stack
import com.twitter.finagle.ssl.OpportunisticTls

/**
 * A class eligible for configuring if a client's TLS mode is opportunistic.
 * If it's not None, then the client will negotiate with the supplied level whether
 * to use TLS or not before setting up TLS.
 *
 * If it's None, it will not attempt to negotiate whether to use TLS or not
 * with the remote peer, and if TLS is configured, the client will communicate over TLS.
 *
 * @note This param will have no effect if the client's protocol does not support
 *       [[OpportunisticTls]].
 */
case class OppTls(level: Option[OpportunisticTls.Level]) {
  def mk(): (OppTls, Stack.Param[OppTls]) =
    (this, OppTls.param)
}

object OppTls {
  implicit val param = new Stack.Param[OppTls] {
    val default: OppTls = OppTls(None)

    // override this to have a "cleaner" output in the registry
    override def show(value: OppTls): Seq[(String, () => String)] = {
      val levelStr = value match {
        case OppTls(Some(oppTls)) => oppTls.value
        case OppTls(None) => "none"
      }
      Seq(("opportunisticTlsLevel", () => levelStr))
    }
  }

  /** Determine whether opportunistic TLS is configured to `Desired` or `Required`. */
  def enabled(params: Stack.Params): Boolean = params[OppTls].level match {
    case Some(OpportunisticTls.Desired | OpportunisticTls.Required) => true
    case Some(OpportunisticTls.Off) | None => false
  }
}
