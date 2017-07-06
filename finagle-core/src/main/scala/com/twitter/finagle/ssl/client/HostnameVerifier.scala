package com.twitter.finagle.ssl.client

import com.twitter.finagle.{Address, SslHostVerificationException}
import com.twitter.util.Try
import java.security.cert.X509Certificate
import javax.net.ssl.SSLSession
import sun.security.util.HostnameChecker

private[finagle] object HostnameVerifier extends SslClientSessionVerifier {
  private[this] val checker = HostnameChecker.getInstance(HostnameChecker.TYPE_TLS)

  /**
   * Run hostname verification on the session.  This will fail with a
   * [[com.twitter.finagle.SslHostVerificationException]] if the certificate is
   * invalid for the given session.
   *
   * This uses [[sun.security.util.HostnameChecker]]. Any bugs are theirs.
   */
  def apply(
    address: Address,
    config: SslClientConfiguration,
    session: SSLSession
  ): Boolean = {
    config.hostname match {
      case Some(host) =>
        // We take the first certificate from the given `getPeerCertificates` array since the expected
        // array structure is peer's own certificate first followed by any certificate authorities.
        val isValid = session.getPeerCertificates.headOption.exists {
          case x509: X509Certificate => Try(checker.`match`(host, x509)).isReturn
          case _ => false
        }

        if (isValid) true
        else throw new SslHostVerificationException(session.getPeerPrincipal.getName)
      case None => SslClientSessionVerifier.AlwaysValid(address, config, session)
    }
  }
}
