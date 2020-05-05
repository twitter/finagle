package com.twitter.finagle.ssl

import com.twitter.finagle.Stack
import com.twitter.io.Buf

// Note that this is a deliberate copy of the Mux OpportunisticTls
// structure with the hope that we can make those configuration options
// extend to all server implementations using TLS snooping.
//
// It is currently private while we examine what the user facing
// API should be.
private[finagle] object OpportunisticTls {

  case class Param(level: Level)

  object Param {
    implicit val param = Stack.Param(Param(Off))
  }

  /**
   * Configures the level of TLS that the client or server can support or must
   * support.
   * @note Java users: See [[OpportunisticTlsConfig]].
   */
  sealed abstract class Level protected (val value: String) {
    val buf: Buf = Buf.Utf8(value)
  }

  /**
   * Indicates that the peer cannot upgrade to tls.
   *
   * Compatible with "off", or "desired".
   */
  case object Off extends Level("off")

  /**
   * Indicates that the peer can upgrade to tls.
   *
   * The peer will upgrade to tls if the remote peer is "desired", or
   * "required", and will stay on cleartext if the remote peer is "off".
   * Compatible with "off", "desired", or "required".
   */
  case object Desired extends Level("desired")

  /**
   * Indicates that the peer must upgrade to tls.
   *
   * Compatible with "desired", or "required".
   */
  case object Required extends Level("required")
}
