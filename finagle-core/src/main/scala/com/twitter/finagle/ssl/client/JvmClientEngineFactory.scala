package com.twitter.finagle.ssl.client

import com.twitter.finagle.Address
import com.twitter.finagle.ssl.{
  Engine, IgnorantTrustManager, KeyCredentials, SslConfigurationException, TrustCredentials}
import com.twitter.util.security.{Pkcs8KeyManagerFactory, X509TrustManagerFactory}
import com.twitter.util.{Return, Throw}
import javax.net.ssl.{KeyManager, SSLContext, TrustManager}

/**
 * This engine factory is a default JVM-based implementation, intended to provide
 * coverage for a wide array of configurations.
 */
private[ssl] case object JvmClientEngineFactory extends SslClientEngineFactory {

  /**
   * Creates an optional array of [[javax.net.ssl.KeyManager]] based on the [[SslClientConfiguration]]
   * passed in. The array should at most contain one item. It is structured in this manner
   * based on the required inputs to [[javax.net.ssl.SSLContext SSLContext's]] `init` method.
   *
   * @note KeyCredentials.Unspecified will return a value of None, which should be turned into null
   * when passed to `init`. In this context, null has meaning to where "the installed security providers
   * will be searched for the highest priority implementation of the appropriate factory."
   *
   * See the `init` method of https://docs.oracle.com/javase/8/docs/api/javax/net/ssl/SSLContext.html
   * for more information.
   */
  private[this] def getKeyManagers(config: SslClientConfiguration): Option[Array[KeyManager]] = {
    config.keyCredentials match {
      case KeyCredentials.Unspecified => None
      case KeyCredentials.CertAndKey(certFile, keyFile) =>
        val factory = new Pkcs8KeyManagerFactory(certFile, keyFile)
        val tryKms = factory.getKeyManagers()
        tryKms match {
          case Return(kms) => Some(kms)
          case Throw(ex) => throw SslConfigurationException(ex.getMessage, ex)
        }
      case KeyCredentials.CertKeyAndChain(certFile, keyFile, chainFile) =>
        throw SslConfigurationException.notSupported(
          "KeyCredentials.CertKeyAndChain", "JvmClientEngineFactory")
    }
  }

  /**
   * Creates an optional array of [[javax.net.ssl.TrustManager]] based on the [[SslClientConfiguration]]
   * passed in. The array should at most contain one item. It is structured in this manner
   * based on the required inputs to [[javax.net.ssl.SSLContext SSLContext's]] `init` method.
   *
   * @note TrustCredentials.Unspecified will return a value of None, which should be turned into null
   * when passed to `init`. In this context, null has meaning to where "the installed security providers
   * will be searched for the highest priority implementation of the appropriate factory."
   *
   * See the `init` method of https://docs.oracle.com/javase/8/docs/api/javax/net/ssl/SSLContext.html
   * for more information.
   */
  private[this] def getTrustManagers(config: SslClientConfiguration): Option[Array[TrustManager]] = {
    config.trustCredentials match {
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
   * Creates an SSLContext and initializes it with [[javax.net.ssl.KeyManager]] and
   * [[javax.net.ssl.TrustManager]] based on the passed in [[SslClientConfiguration]].
   *
   * @note TLSv1.2 is specified as the protocol here, because it's the latest one
   * which Java 8 supports. Specifying TLSv1.2 here though does not mean that "only"
   * TLSv1.2 will be supported by the created engine. Calling `getSupportedProtocols`
   * on the created [[javax.net.ssl.SSLEngine]] will return
   * Array(SSLv2Hello, SSLv3, TLSv1, TLSv1.1, TLSv1.2).
   *
   * See https://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#SSLContext
   * for more information.
   */
  private[this] def initializeSslContext(config: SslClientConfiguration): SSLContext = {
    val sslContext = SSLContext.getInstance("TLSv1.2")
    sslContext.init(getKeyManagers(config).orNull, getTrustManagers(config).orNull, null)
    sslContext
  }

  /**
   * Creates a new [[Engine]] based on an [[Address]] and an [[SslClientConfiguration]].
   *
   * @param address A physical address which potentially includes metadata.
   *
   * @param config A collection of parameters which the engine factory should
   * consider when creating the TLS client [[Engine]].
   *
   * @note [[ApplicationProtocols]] other than Unspecified are not supported.
   */
  def mkEngine(address: Address, config: SslClientConfiguration): Engine = {
    SslClientEngineFactory.checkApplicationProtocolsNotSupported("JvmClientEngineFactory", config)
    val sslContext = initializeSslContext(config)
    val engine = SslClientEngineFactory.createEngine(sslContext, address, config)
    SslClientEngineFactory.configureEngine(engine, config)
    engine
  }

}
