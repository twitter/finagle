package com.twitter.finagle.ssl

import com.twitter.util.security.Pkcs8KeyManagerFactory
import com.twitter.util.security.X509TrustManagerFactory
import com.twitter.util.Return
import com.twitter.util.Throw
import java.io.File
import javax.net.ssl.KeyManager
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLEngine
import javax.net.ssl.TrustManager

private[ssl] object SslConfigurations {

  private def filesToKeyManagers(certsFile: File, keyFile: File): Array[KeyManager] = {
    val factory = new Pkcs8KeyManagerFactory(certsFile, keyFile)
    val tryKms = factory.getKeyManagers()
    tryKms match {
      case Return(kms) => kms
      case Throw(ex) => throw SslConfigurationException(ex)
    }
  }

  /**
   * Creates an optional array of `javax.net.ssl.KeyManager` based on the [[KeyCredentials]]
   * passed in. The array should at most contain one item. It is structured in this manner
   * based on the required inputs to `javax.net.ssl.SSLContext SSLContext`'s `init` method.
   *
   * @note KeyCredentials.Unspecified will return a value of None, which should be turned into
   * null when passed to `init`. In this context, null has meaning to where "the installed
   * security providers will be searched for the highest priority implementation of the appropriate factory."
   *
   * See the `init` method of https://docs.oracle.com/javase/8/docs/api/javax/net/ssl/SSLContext.html
   * for more information.
   */
  def getKeyManagers(keyCredentials: KeyCredentials): Option[Array[KeyManager]] =
    keyCredentials match {
      case KeyCredentials.Unspecified => None
      case KeyCredentials.CertAndKey(certFile, keyFile) =>
        Some(filesToKeyManagers(certFile, keyFile))
      case KeyCredentials.CertsAndKey(certsFile, keyFile) =>
        Some(filesToKeyManagers(certsFile, keyFile))
      case _: KeyCredentials.CertKeyAndChain =>
        throw SslConfigurationException.notSupported(
          "KeyCredentials.CertKeyAndChain",
          "SslConfigurations"
        )
      case KeyCredentials.KeyManagerFactory(keyManagerFactory) =>
        Some(keyManagerFactory.getKeyManagers)
    }

  /**
   * Creates an optional array of `javax.net.ssl.TrustManager` based on the [[TrustCredentials]]
   * passed in. The array should at most contain one item. It is structured in this manner
   * based on the required inputs to `javax.net.ssl.SSLContext SSLContext's` `init` method.
   *
   * @note TrustCredentials.Unspecified will return a value of None, which should be turned into null
   * when passed to `init`. In this context, null has meaning to where "the installed security providers
   * will be searched for the highest priority implementation of the appropriate factory."
   *
   * See the `init` method of https://docs.oracle.com/javase/8/docs/api/javax/net/ssl/SSLContext.html
   * for more information.
   */
  def getTrustManagers(trustCredentials: TrustCredentials): Option[Array[TrustManager]] =
    trustCredentials match {
      case TrustCredentials.Unspecified => None
      case TrustCredentials.Insecure => Some(Array(new IgnorantTrustManager))
      case TrustCredentials.CertCollection(file) =>
        val factory = new X509TrustManagerFactory(file)
        val tryTms = factory.getTrustManagers()
        tryTms match {
          case Return(tms) => Some(tms)
          case Throw(ex) => throw SslConfigurationException(ex)
        }
      case TrustCredentials.X509Certificates(x509Certs) =>
        val tryTms = X509TrustManagerFactory.buildTrustManager(x509Certs)
        tryTms match {
          case Return(tms) => Some(tms)
          case Throw(ex) => throw SslConfigurationException(ex)
        }
      case TrustCredentials.TrustManagerFactory(trustManagerFactory) =>
        Some(trustManagerFactory.getTrustManagers)
    }

  /**
   * Creates an SSLContext and initializes it with `javax.net.ssl.KeyManager` and
   * `javax.net.ssl.TrustManager` based on the passed in [[KeyCredentials]] and
   * [[TrustCredentials]].
   *
   * @note TLSv1.3 is specified as the protocol here, because it's the latest one
   * which Java 8 supports. Specifying TLSv1.3 here though does not mean that "only"
   * TLSv1.3 will be supported by the created engine. Calling `getSupportedProtocols`
   * on the created `javax.net.ssl.SSLEngine` will return
   * Array(SSLv2Hello, SSLv3, TLSv1, TLSv1.1, TLSv1.2, TLSv1.3).
   *
   * See https://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#SSLContext
   * for more information.
   */
  def initializeSslContext(
    keyCredentials: KeyCredentials,
    trustCredentials: TrustCredentials
  ): SSLContext = {
    val sslContext = SSLContext.getInstance("TLSv1.3")
    sslContext.init(
      getKeyManagers(keyCredentials).orNull,
      getTrustManagers(trustCredentials).orNull,
      null
    )
    sslContext
  }

  /**
   * Sets the enabled cipher suites of the supplied
   * `javax.net.ssl.SSLEngine`.
   */
  def configureCipherSuites(sslEngine: SSLEngine, cipherSuites: CipherSuites): Unit =
    cipherSuites match {
      case CipherSuites.Unspecified => // Do Nothing
      case CipherSuites.Enabled(ciphers) =>
        sslEngine.setEnabledCipherSuites(ciphers.toArray)
    }

  /**
   * Sets the enabled protocols of the supplied
   * `javax.net.ssl.SSLEngine`.
   */
  def configureProtocols(sslEngine: SSLEngine, protocols: Protocols): Unit =
    protocols match {
      case Protocols.Unspecified => // Do Nothing
      case Protocols.Enabled(protocols) =>
        sslEngine.setEnabledProtocols(protocols.toArray)
    }

  /**
   * If a non-empty hostname is supplied, the engine is set to use
   * "HTTPS"-style hostname verification.
   *
   * See: `javax.net.ssl.SSLParameters#setEndpointIdentificationAlgorithm` and
   * https://docs.oracle.com/javase/7/docs/technotes/guides/security/StandardNames.html
   * for more details.
   */
  def configureHostnameVerification(sslEngine: SSLEngine, hostname: Option[String]): Unit =
    hostname match {
      case Some(_) =>
        val sslParameters = sslEngine.getSSLParameters()
        sslParameters.setEndpointIdentificationAlgorithm("HTTPS")
        sslEngine.setSSLParameters(sslParameters)
      case None => // do nothing
    }

  /**
   * Guard method for failing fast inside of a factory's apply method when
   * [[KeyCredentials]] are not supported.
   */
  def checkKeyCredentialsNotSupported(
    engineFactoryName: String,
    keyCredentials: KeyCredentials
  ): Unit =
    keyCredentials match {
      case KeyCredentials.Unspecified => // Do Nothing
      case KeyCredentials.CertAndKey(_, _) =>
        throw SslConfigurationException.notSupported("KeyCredentials.CertAndKey", engineFactoryName)
      case KeyCredentials.CertsAndKey(_, _) =>
        throw SslConfigurationException.notSupported(
          "KeyCredentials.CertsAndKey",
          engineFactoryName
        )
      case KeyCredentials.CertKeyAndChain(_, _, _) =>
        throw SslConfigurationException.notSupported(
          "KeyCredentials.CertKeyAndChain",
          engineFactoryName
        )
      case KeyCredentials.KeyManagerFactory(_) =>
        throw SslConfigurationException.notSupported(
          "KeyCredentials.KeyManagerFactory",
          engineFactoryName
        )
    }

  /**
   * Guard method for failing fast inside of a factory's apply method when
   * [[TrustCredentials]] are not supported.
   */
  def checkTrustCredentialsNotSupported(
    engineFactoryName: String,
    trustCredentials: TrustCredentials
  ): Unit =
    trustCredentials match {
      case TrustCredentials.Unspecified => // Do Nothing
      case TrustCredentials.Insecure =>
        throw SslConfigurationException.notSupported("TrustCredentials.Insecure", engineFactoryName)
      case TrustCredentials.CertCollection(_) =>
        throw SslConfigurationException.notSupported(
          "TrustCredentials.CertCollection",
          engineFactoryName
        )
      case TrustCredentials.X509Certificates(_) =>
        throw SslConfigurationException.notSupported(
          "TrustCredentials.X509Certificates",
          engineFactoryName
        )
      case TrustCredentials.TrustManagerFactory(_) =>
        throw SslConfigurationException.notSupported(
          "TrustCredentials.TrustManagerFactory",
          engineFactoryName
        )
    }

  /**
   * Guard method for failing fast inside of a factory's apply method when
   * [[Protocols]] are not supported.
   */
  def checkProtocolsNotSupported(engineFactoryName: String, protocols: Protocols): Unit =
    protocols match {
      case Protocols.Unspecified => // Do Nothing
      case Protocols.Enabled(_) =>
        throw SslConfigurationException.notSupported("Protocols.Enabled", engineFactoryName)
    }

  /**
   * Guard method for failing fast inside of a factory's apply method when
   * [[ApplicationProtocols]] are not supported.
   */
  def checkApplicationProtocolsNotSupported(
    engineFactoryName: String,
    applicationProtocols: ApplicationProtocols
  ): Unit =
    applicationProtocols match {
      case ApplicationProtocols.Unspecified => // Do Nothing
      case ApplicationProtocols.Supported(_) =>
        throw SslConfigurationException.notSupported(
          "ApplicationProtocols.Supported",
          engineFactoryName
        )
    }

  /**
   * Guard method for failing fast inside of a server factory's apply method when
   * [[ClientAuth]] is not supported.
   */
  def checkClientAuthNotSupported(engineFactoryName: String, clientAuth: ClientAuth): Unit =
    clientAuth match {
      case ClientAuth.Unspecified => // Do Nothing
      case ClientAuth.Off =>
        throw SslConfigurationException.notSupported("ClientAuth.Off", engineFactoryName)
      case ClientAuth.Wanted =>
        throw SslConfigurationException.notSupported("ClientAuth.Wanted", engineFactoryName)
      case ClientAuth.Needed =>
        throw SslConfigurationException.notSupported("ClientAuth.Needed", engineFactoryName)
    }

}
