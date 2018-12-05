package com.twitter.finagle.ssl.client

import com.twitter.finagle.{Address, Stack}
import javax.net.ssl.SSLSession

/**
 * SslClientSessionVerifier represents an opportunity for a user or system
 * to perform additional client-side verification checks against an address,
 * configuration, or session. The `apply` method of the verifier is called
 * when an `SSLSession` has been established.
 */
abstract class SslClientSessionVerifier {

  /**
   * Verifies the established `SSLSession`.
   *
   * @return true if the session is verified, false if not.
   * @note Throwing an exception is ok, and will be treated
   * similarly to a response of false.
   */
  def apply(address: Address, config: SslClientConfiguration, session: SSLSession): Boolean
}

object SslClientSessionVerifier {

  /**
   * @param verifier The [[SslClientSessionVerifier]] whose `apply` method
   * is called when an `SSLSession` is established. This method determines
   * whether the SSL/TLS connection should be used. It is an additional level
   * of verification beyond the standard certificate chain verification checking.
   *
   * @note By default sessions will be seen as `AlwaysValid` if this
   * param is not configured.
   */
  case class Param(verifier: SslClientSessionVerifier) {
    def mk(): (Param, Stack.Param[Param]) =
      (this, Param.param)
  }
  object Param {
    implicit val param = Stack.Param(Param(AlwaysValid))
  }

  /**
   * An [[SslClientSessionVerifier]] that blindly
   * verifies every given session.
   */
  val AlwaysValid: SslClientSessionVerifier = new SslClientSessionVerifier {
    def apply(address: Address, config: SslClientConfiguration, session: SSLSession): Boolean = true
  }

}
