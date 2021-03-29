package com.twitter.finagle.ssl

import java.io.File
import java.security.cert.X509Certificate
import javax.net.ssl

/**
 * TrustCredentials represent the items necessary for this configured
 * TLS [[Engine]] to verify a remote peer's credentials.
 *
 * @note Java users: See [[TrustCredentialsConfig]].
 */
sealed trait TrustCredentials

object TrustCredentials {

  /**
   * Indicates that the trust credentials to use with a particular
   * engine should be delegated to the engine factory, or that none
   * are necessary.
   */
  case object Unspecified extends TrustCredentials

  /**
   * Indicates that a remote peer's credentials should not be
   * verified. This value is not recommended for use outside of
   * development and testing.
   */
  case object Insecure extends TrustCredentials

  /**
   * Indicates the collection of certificates which should be used in
   * verifying a remote peer's credentials can be read from the given
   * file.
   *
   * @param file A file containing a collection of X.509 certificates
   *             in PEM format.
   */
  case class CertCollection(file: File) extends TrustCredentials

  /**
   * The collection of certificates which should be used in
   * verifying a remote peer's credentials.
   *
   * @param x509Certs A collection of X.509 certificates
   */
  case class X509Certificates(x509Certs: Seq[X509Certificate]) extends TrustCredentials

  /**
   * Indicates that the trust credentials from the [[ssl.TrustManagerFactory]]
   * should be used in verifying a remote peer's credentials.
   *
   * @param trustManagerFactory the factory delivering the TrustManager for
   *                            validation
   */
  case class TrustManagerFactory(trustManagerFactory: ssl.TrustManagerFactory)
      extends TrustCredentials

}
