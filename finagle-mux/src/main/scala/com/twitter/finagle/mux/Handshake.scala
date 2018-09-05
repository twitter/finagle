package com.twitter.finagle.mux

import com.twitter.io.Buf

/**
 * The mux spec allows for (re)negotiation to happen arbitrarily throughout a
 * session, but for simplicity, our implementation assumes it happens at the
 * start of a session.
 */
private[finagle] object Handshake {
  type Headers = Seq[(Buf, Buf)]

  /**
   * Returns Some(value) if `key` exists in `headers`, otherwise None.
   */
  def valueOf(key: Buf, headers: Headers): Option[Buf] = {
    val iter = headers.iterator
    while (iter.hasNext) {
      val (k, v) = iter.next()
      if (k == key) return Some(v)
    }
    None
  }

  /**
   * We can assign tag 1 without worry of any tag conflicts because we gate
   * all messages until the handshake is complete (or fails).
   */
  val TinitTag = 1

  /**
   * Unfortunately, `Rerr` messages don't have error codes in the current
   * version of mux. This means that we need to match strings to distinguish
   * between important `Rerr` messages. This one is particularly important
   * because it allows us to roll out handshakes without a coordinated
   * upgrade path.
   */
  val CanTinitMsg = "tinit check"
}
