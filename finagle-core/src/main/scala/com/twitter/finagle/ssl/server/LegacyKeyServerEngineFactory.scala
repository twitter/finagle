package com.twitter.finagle.ssl.server

import com.twitter.finagle.ssl._
import com.twitter.util.{Return, Throw, Try}
import javax.net.ssl.{KeyManager, SSLContext}

/**
 * This engine factory is intended to act as a temporary bridge for those currently using
 * non-PKCS#8 PEM-encoded keys, such as PKCS#1 keys. Once the private key file has been
 * converted into PKCS#8 PEM-encoded format, [[JdkServerEngineFactory]] or
 * `Netty4ServerEngineFactory` should be used instead.
 *
 * @note This engine factory uses [[PEMEncodedKeyManager]] which shells out to OpenSSL
 * in order to convert the key into a usable format.
 */
@deprecated("Use Netty4ServerEngineFactory instead", "2017-02-10")
object LegacyKeyServerEngineFactory extends SslServerEngineFactory {

  /**
   * Creates an optional array of `javax.net.ssl.KeyManager` based on the [[KeyCredentials]]
   * passed in. The array should at most contain one item. This differs from
   * [[SslConfigurations]] `getKeyManagers` method in that the legacy [[PEMEncodedKeyManager]]
   * is used to convert a non-PKCS#8 key into a usable format.
   */
  private def getKeyManagers(keyCredentials: KeyCredentials): Option[Array[KeyManager]] =
    keyCredentials match {
      case KeyCredentials.Unspecified => None
      case KeyCredentials.CertAndKey(certFile, keyFile) =>
        val tryKms = Try(PEMEncodedKeyManager(
          certFile.getAbsolutePath(), keyFile.getAbsolutePath(), None))
        tryKms match {
          case Return(kms) => Some(kms)
          case Throw(ex) => throw SslConfigurationException(ex.getMessage, ex)
        }
      case KeyCredentials.CertKeyAndChain(certFile, keyFile, chainFile) =>
        val tryKms = Try(PEMEncodedKeyManager(
          certFile.getAbsolutePath(),
          keyFile.getAbsolutePath(),
          Some(chainFile.getAbsolutePath())))
        tryKms match {
          case Return(kms) => Some(kms)
          case Throw(ex) => throw SslConfigurationException(ex.getMessage, ex)
        }
    }

  /**
   * A slightly different implementation of [[SslConfigurations]] `initializeSslContext`
   * to use the [[PEMEncodedKeyManager]] provided key managers instead.
   *
   * @note See [[SslConfigurations]] `initializeSslContext` for more details.
   */
  private def initializeSslContext(
    keyCredentials: KeyCredentials,
    trustCredentials: TrustCredentials
  ): SSLContext = {
    val sslContext = SSLContext.getInstance("TLSv1.2")
    sslContext.init(
      getKeyManagers(keyCredentials).orNull,
      SslConfigurations.getTrustManagers(trustCredentials).orNull,
      null)
    sslContext
  }

  /**
   * Creates a new [[Engine]] based on an [[SslServerConfiguration]].
   *
   * @param config A collection of parameters which the engine factory
   * should consider when creating the TLS server [[Engine]].
   *
   * @note This engine should only be used when using key formats which
   * are not PKCS#8 PEM-encoded.
   *
   * @note [[ApplicationProtocols]] other than Unspecified are not supported.
   */
  def apply(config: SslServerConfiguration): Engine = {
    SslConfigurations.checkApplicationProtocolsNotSupported(
      "LegacyKeyServerEngineFactory", config.applicationProtocols)

    val sslContext = initializeSslContext(
      config.keyCredentials, config.trustCredentials)
    val engine = SslServerEngineFactory.createEngine(sslContext)
    SslServerEngineFactory.configureEngine(engine, config)
    engine
  }

}
