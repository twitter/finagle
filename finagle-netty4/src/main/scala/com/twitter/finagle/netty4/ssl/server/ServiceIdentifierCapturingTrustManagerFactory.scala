package com.twitter.finagle.netty4.ssl.server

import java.security.KeyStore
import javax.net.ssl.ManagerFactoryParameters
import javax.net.ssl.TrustManager
import javax.net.ssl.TrustManagerFactory
import javax.net.ssl.TrustManagerFactorySpi
import javax.net.ssl.X509TrustManager

/**
 * A TrustManagerFactory that wraps another factory and injects ServiceIdentifierCapturingTrustManager
 * to capture service identifiers during certificate verification.
 */
class ServiceIdentifierCapturingTrustManagerFactory(delegate: TrustManagerFactory)
    extends TrustManagerFactory(
      new ServiceIdentifierCapturingTrustManagerFactorySpi(delegate),
      delegate.getProvider,
      delegate.getAlgorithm
    )

private class ServiceIdentifierCapturingTrustManagerFactorySpi(delegate: TrustManagerFactory)
    extends TrustManagerFactorySpi {

  override def engineInit(ks: KeyStore): Unit = {
    delegate.init(ks)
  }

  override def engineInit(spec: ManagerFactoryParameters): Unit = {
    delegate.init(spec)
  }

  override def engineGetTrustManagers(): Array[TrustManager] = {
    delegate.getTrustManagers.map {
      case x509tm: X509TrustManager =>
        new ServiceIdentifierCapturingTrustManager(x509tm)
      case other => other
    }
  }
}
