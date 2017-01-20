package com.twitter.finagle.ssl

import com.twitter.util.security.{Pkcs8KeyManagerFactory, X509TrustManagerFactory}
import com.twitter.util.{Return, Throw}
import javax.net.ssl.{KeyManager, SSLContext, SSLEngine, TrustManager}

private[ssl] object SslConfigurations {

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
  private def getKeyManagers(keyCredentials: KeyCredentials): Option[Array[KeyManager]] = {
    keyCredentials match {
      case KeyCredentials.Unspecified => None
      case KeyCredentials.CertAndKey(certFile, keyFile) =>
        val factory = new Pkcs8KeyManagerFactory(certFile, keyFile)
        val tryKms = factory.getKeyManagers()
        tryKms match {
          case Return(kms) => Some(kms)
          case Throw(ex) => throw SslConfigurationException(ex.getMessage, ex)
        }
      case _: KeyCredentials.CertKeyAndChain =>
        throw SslConfigurationException.notSupported(
          "KeyCredentials.CertKeyAndChain", "SslConfigurations")
    }
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
  private def getTrustManagers(trustCredentials: TrustCredentials): Option[Array[TrustManager]] = {
    trustCredentials match {
      case TrustCredentials.Unspecified => None
      case TrustCredentials.Insecure => Some(Array(new IgnorantTrustManager))
      case TrustCredentials.CertCollection(file) =>
        val factory = new X509TrustManagerFactory(file)
        val tryTms = factory.getTrustManagers()
        tryTms match {
          case Return(tms) => Some(tms)
          case Throw(ex) => throw SslConfigurationException(ex.getMessage, ex)
        }
    }
  }

  /**
   * Creates an SSLContext and initializes it with `javax.net.ssl.KeyManager` and
   * `javax.net.ssl.TrustManager` based on the passed in [[KeyCredentials]] and
   * [[TrustCredentials]].
   *
   * @note TLSv1.2 is specified as the protocol here, because it's the latest one
   * which Java 8 supports. Specifying TLSv1.2 here though does not mean that "only"
   * TLSv1.2 will be supported by the created engine. Calling `getSupportedProtocols`
   * on the created `javax.net.ssl.SSLEngine` will return
   * Array(SSLv2Hello, SSLv3, TLSv1, TLSv1.1, TLSv1.2).
   *
   * See https://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#SSLContext
   * for more information.
   */
  def initializeSslContext(
    keyCredentials: KeyCredentials,
    trustCredentials: TrustCredentials
  ): SSLContext = {
    val sslContext = SSLContext.getInstance("TLSv1.2")
    sslContext.init(
      getKeyManagers(keyCredentials).orNull,
      getTrustManagers(trustCredentials).orNull,
      null)
    sslContext
  }

  /**
   * Sets the enabled cipher suites of the supplied
   * `javax.net.ssl.SSLEngine`.
   */
  def configureCipherSuites(
    sslEngine: SSLEngine,
    cipherSuites: CipherSuites
  ): Unit = {
    cipherSuites match {
      case CipherSuites.Unspecified => // Do Nothing
      case CipherSuites.Enabled(ciphers) =>
        sslEngine.setEnabledCipherSuites(ciphers.toArray)
    }
  }

  /**
   * Sets the enabled protocols of the supplied
   * `javax.net.ssl.SSLEngine`.
   */
  def configureProtocols(
    sslEngine: SSLEngine,
    protocols: Protocols
  ): Unit = {
    protocols match {
      case Protocols.Unspecified => // Do Nothing
      case Protocols.Enabled(protocols) =>
        sslEngine.setEnabledProtocols(protocols.toArray)
    }
  }

  /**
   * Guard method for failing fast inside of a factory's apply method when
   * [[KeyCredentials]] are not supported.
   */
  def checkKeyCredentialsNotSupported(
    engineFactoryName: String,
    keyCredentials: KeyCredentials
  ): Unit = {
    keyCredentials match {
      case KeyCredentials.Unspecified => // Do Nothing
      case KeyCredentials.CertAndKey(_, _) =>
        throw SslConfigurationException.notSupported(
          "KeyCredentials.CertAndKey", engineFactoryName)
      case KeyCredentials.CertKeyAndChain(_, _, _) =>
        throw SslConfigurationException.notSupported(
          "KeyCredentials.CertKeyAndChain", engineFactoryName)
    }
  }

  /**
   * Guard method for failing fast inside of a factory's apply method when
   * [[TrustCredentials]] are not supported.
   */
  def checkTrustCredentialsNotSupported(
    engineFactoryName: String,
    trustCredentials: TrustCredentials
  ): Unit = {
    trustCredentials match {
      case TrustCredentials.Unspecified => // Do Nothing
      case TrustCredentials.Insecure =>
        throw SslConfigurationException.notSupported(
          "TrustCredentials.Insecure", engineFactoryName)
      case TrustCredentials.CertCollection(_) =>
        throw SslConfigurationException.notSupported(
          "TrustCredentials.CertCollection", engineFactoryName)
    }
  }

  /**
   * Guard method for failing fast inside of a factory's apply method when
   * [[ApplicationProtocols]] are not supported.
   */
  def checkApplicationProtocolsNotSupported(
    engineFactoryName: String,
    applicationProtocols: ApplicationProtocols
  ): Unit = {
    applicationProtocols match {
      case ApplicationProtocols.Unspecified => // Do Nothing
      case ApplicationProtocols.Supported(_) =>
        throw SslConfigurationException.notSupported(
          "ApplicationProtocols.Supported", engineFactoryName)
    }
  }

}
