package com.twitter.finagle.mux

import com.twitter.finagle.Mux.param
import com.twitter.finagle.Stack
import com.twitter.finagle.ssl.OpportunisticTls

/**
 * Mixin for supporting Opportunistic Tls on clients and servers
 */
trait OpportunisticTlsParams[A <: Stack.Parameterized[A]] { self: Stack.Parameterized[A] =>

  /**
   * Configures whether to speak TLS or not.
   *
   * By default, don't use opportunistic TLS, and instead try to speak mux over TLS
   * if TLS has been configured.
   *
   * The valid levels are Off, which indicates this will never speak TLS,
   * Desired, which indicates it may speak TLS, but may also not speak TLS,
   * and Required, which indicates it must speak TLS.
   *
   * Peers that are configured with level `Required` cannot speak to peers that are
   * configured with level `Off`.
   *
   * Note that opportunistic TLS is negotiated in a cleartext handshake, and is
   * incompatible with mux over TLS.
   */
  def withOpportunisticTls(level: OpportunisticTls.Level): A =
    configured(param.OppTls(Some(level)))

  /**
   * Disables opportunistic TLS.
   *
   * If this is still TLS configured, it will speak mux over TLS.  To instead
   * configure this to be `Off`, use `withOpportunisticTls(OpportunisticTls.Off)`.
   */
  def withNoOpportunisticTls: A = configured(param.OppTls(None))

}
