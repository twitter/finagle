package com.twitter.finagle.ssl

import com.twitter.finagle.SslHostVerificationException
import com.twitter.util.Try
import sun.security.util.HostnameChecker
import java.security.cert.X509Certificate
import javax.net.ssl.SSLSession

/**
 * Performs [[SSLSession]] validation defined as a function `SSLSession => Option[Throwable]`, where
 * `Some(exception)` means a failed validation with `exception`.
 */
private[finagle] abstract class SessionVerifier extends (SSLSession => Option[Throwable])

private[finagle] object SessionVerifier {

  /**
   * A [[SessionVerifier]] that blindly validates every given session.
   */
  val AlwaysValid: SessionVerifier = new SessionVerifier {
    override def apply(session: SSLSession): Option[Throwable] = None
  }

  /**
   * Run hostname verification on the session.  This will fail with a
   * [[com.twitter.finagle.SslHostVerificationException]] if the certificate is
   * invalid for the given session.
   *
   * This uses [[sun.security.util.HostnameChecker]]. Any bugs are theirs.
   */
  def hostname(hostname: String): SessionVerifier = new SessionVerifier {
    val checker = HostnameChecker.getInstance(HostnameChecker.TYPE_TLS)

    override def apply(session: SSLSession): Option[Throwable] = {
      // We take the first certificate from the given `getPeerCertificates` array since the expected
      // array structure is peer's own certificate first followed by any certificate authorities.
      val isValid = session.getPeerCertificates.headOption.exists {
        case x509: X509Certificate => Try(checker.`match`(hostname, x509)).isReturn
        case _ => false
      }

      if (isValid) None
      else Some(new SslHostVerificationException(session.getPeerPrincipal.getName))
    }
  }
}