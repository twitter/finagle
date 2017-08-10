package com.twitter.finagle.mux.transport

import com.twitter.io.Buf
import com.twitter.logging.Logger

private[finagle] object OpportunisticTls {
  private[this] val log = Logger.get()

  /**
   * Defines encrypter keys and values exchanged as part of a
   * mux session header during initialization.
   */
  object Header {
    val KeyBuf: Buf = Buf.Utf8("tls")

    /**
     * Extracts level from the `buf`.
     */
    def decodeLevel(buf: Buf): Level =
      if (buf == Off.buf) Off
      else if (buf == Desired.buf) Desired
      else if (buf == Required.buf) Required
      else {
        val Buf.Utf8(bad) = buf
        log.debug(s"Expected one of 'off', 'desired', or 'required' but received $bad")
        Off // don't want to fail in case we decide to change levels in the future.
      }
  }

  /**
   * Configures the level of TLS that the client or server can support or must
   * support.
   */
  sealed abstract class Level(value: String) {
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
