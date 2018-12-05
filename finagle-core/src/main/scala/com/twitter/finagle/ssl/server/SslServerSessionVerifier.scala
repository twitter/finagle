package com.twitter.finagle.ssl.server

import com.twitter.finagle.{Address, Stack}
import javax.net.ssl.SSLSession

/**
 * SslServerSessionVerifier represents an opportunity for a user or system
 * to perform additional server-side verification checks against a
 * configuration or session. The `apply` method of the verifier is called
 * when an `SSLSession` has been established.
 */
abstract class SslServerSessionVerifier {

  /**
   * Verifies the established `SSLSession`.
   *
   * @return true if the session is verified, false if not.
   * @note Throwing an exception is ok, and will be treated
   * similarly to a response of false.
   */
  def apply(address: Address, config: SslServerConfiguration, session: SSLSession): Boolean
}

object SslServerSessionVerifier {

  /**
   * @param verifier The [[SslServerSessionVerifier]] whose `apply` method
   * is called when an `SSLSession` is established. This method determines
   * whether the SSL/TLS connection should be used. It is an additional level
   * of verification beyond the standard certificate chain verification checking.
   *
   * @note By default sessions will be seen as `AlwaysValid` if this
   * param is not configured.
   */
  case class Param(verifier: SslServerSessionVerifier) {
    def mk(): (Param, Stack.Param[Param]) =
      (this, Param.param)
  }
  object Param {
    implicit val param = Stack.Param(Param(AlwaysValid))
  }

  /**
   * An [[SslServerSessionVerifier]] that blindly
   * verifies every given session.
   */
  val AlwaysValid: SslServerSessionVerifier = new SslServerSessionVerifier {
    def apply(address: Address, config: SslServerConfiguration, session: SSLSession): Boolean = true
  }

}
