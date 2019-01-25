package com.twitter.finagle.ssl

import java.io.File
import javax.net.ssl

/**
 * KeyCredentials represent the items necessary for this configured
 * TLS [[Engine]] to authenticate itself to a remote peer. This
 * generally includes an X.509 certificate and a private key.
 *
 * @note Java users: See [[KeyCredentialsConfig]].
 */
sealed trait KeyCredentials

object KeyCredentials {

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
  case class CertAndKey(certificateFile: File, keyFile: File) extends KeyCredentials

  /**
   * Indicates that this certificate chain and key should be used by the
   * engine factory.
   *
   * @param certificatesFile A file containing multiple X.509 certificates in
   * PEM format that are part of the same certificate chain.
   *
   * @param keyFile A file containing a PKCS #8 private key in PEM format.
   * It should not require a password.
   */
  case class CertsAndKey(certificatesFile: File, keyFile: File) extends KeyCredentials

  /**
   * Indicates that this certificate, key, and certificate chain
   * should be used by the engine factory.
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
  case class CertKeyAndChain(certificateFile: File, keyFile: File, caCertificateFile: File)
      extends KeyCredentials

  /**
   * Indicates that this [[ssl.KeyManagerFactory]] should be used by
   * the engine factory.
   * @param keyManagerFactory A factory able of constructing the KeyManagers
   */
  case class KeyManagerFactory(keyManagerFactory: ssl.KeyManagerFactory) extends KeyCredentials
}
