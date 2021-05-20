package com.twitter.finagle.ssl

import com.twitter.io.Buf

object OpportunisticTls {

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
   * Indicates that the peer can upgrade to tls or supports tls snooping.
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

  /** The sequence of [[Level]]s from least to most secure */
  final val Values: Seq[Level] = Seq(Off, Desired, Required)
}
