package com.twitter.finagle.ssl

import java.io.File

/**
 * KeyCredentials represent the items necessary for this configured
 * TLS [[Engine]] to authenticate itself to a remote peer. This
 * generally includes an X.509 certificate and a private key.
 */
private[finagle] sealed trait KeyCredentials

private[finagle] object KeyCredentials {

  /**
   * Indicates that the key credentials to use with the particular
   * engine should be delegated to the engine factory, or that
   * none are necessary (e.g. for a client where client authentication
   * is not used).
   */
  case object Unspecified extends KeyCredentials

  /**
   * Indicates that this certificate and key should be used by the
   * engine factory.
   *
   * @param certificateFile A file containing an X.509 certificate in
   * PEM format.
   *
   * @param keyFile A file containing a PKCS #8 private key in PEM format.
   * It should not require a password.
   */
  case class CertAndKey(
      certificateFile: File,
      keyFile: File)
    extends KeyCredentials

  /**
   * Indicates that this certificate, key, and certificate chain
   * should be used by the engine factory. This option should
   * only be used with legacy engine factories.
   *
   * @param certificateFile A file containing an X.509 certificate in
   * PEM format.
   *
   * @param keyFile A file containing a PKCS #8 private key in PEM format.
   * It should not require a password.
   *
   * @param caCertificateFile A file containing a chain of X.509 certificates
   * in PEM format.
   */
  case class CertKeyAndChain(
      certificateFile: File,
      keyFile: File,
      caCertificateFile: File)
    extends KeyCredentials
}
