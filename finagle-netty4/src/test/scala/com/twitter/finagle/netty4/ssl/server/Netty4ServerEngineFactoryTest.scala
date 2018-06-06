package com.twitter.finagle.netty4.ssl.server

import com.twitter.finagle.ssl._
import com.twitter.finagle.ssl.server.SslServerConfiguration
import com.twitter.io.TempFile
import java.io.File
import java.security.KeyStore
import javax.net.ssl.{KeyManagerFactory, TrustManagerFactory}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Netty4ServerEngineFactoryTest extends FunSuite {

  // Force JDK version for tests, because the native engine could fail to load in different
  // environments
  private[this] val factory = Netty4ServerEngineFactory(forceJdk = true)

  // deleteOnExit for these is handled by TempFile
  private[this] val certFile = TempFile.fromResourcePath("/ssl/certs/svc-test-server.cert.pem")
  private[this] val expiredCertFile = TempFile.fromResourcePath("/ssl/certs/svc-test-server-expired.cert.pem")
  private[this] val keyFile = TempFile.fromResourcePath("/ssl/keys/svc-test-server-pkcs8.key.pem")

  // This file contains multiple certificates
  private[this] val chainFile = TempFile.fromResourcePath("/ssl/certs/svc-test-chain.cert.pem")

  private[this] val goodKeyCredentials = KeyCredentials.CertAndKey(certFile, keyFile)

  test("default config fails") {
    // Netty 4's SslContextBuilder requires key credentials to be specified
    // so leaving them as Unspecified won't work.
    val config = SslServerConfiguration()

    intercept[SslConfigurationException] {
      val engine = factory(config)
    }
  }

  test("config with good cert and key credentials succeeds") {
    val config = SslServerConfiguration(keyCredentials = goodKeyCredentials)
    val engine = factory(config)
    val sslEngine = engine.self

    assert(sslEngine != null)
  }

  test("config with bad cert or key credential fails") {
    val tempCertFile = File.createTempFile("test", "crt")
    tempCertFile.deleteOnExit()

    val keyCredentials = KeyCredentials.CertAndKey(tempCertFile, keyFile)
    val config = SslServerConfiguration(keyCredentials = keyCredentials)

    intercept[SslConfigurationException] {
      val engine = factory(config)
    }
  }

  test("config with expired cert and valid key credential fails") {
    val keyCredentials = KeyCredentials.CertAndKey(expiredCertFile, keyFile)
    val config = SslServerConfiguration(keyCredentials = keyCredentials)

    intercept[SslConfigurationException] {
      factory(config)
    }
  }

  test("config with cert, key, and chain succeeds") {
    val keyCredentials = KeyCredentials.CertKeyAndChain(certFile, keyFile, chainFile)
    val config = SslServerConfiguration(keyCredentials = keyCredentials)

    val engine = factory(config)
    val sslEngine = engine.self

    assert(sslEngine != null)
  }

  test("config with insecure trust credentials succeeds") {
    val config = SslServerConfiguration(
      keyCredentials = goodKeyCredentials,
      trustCredentials = TrustCredentials.Insecure
    )
    val engine = factory(config)
    val sslEngine = engine.self

    assert(sslEngine != null)
  }

  test("config with good trusted cert collection succeeds") {
    val trustCredentials = TrustCredentials.CertCollection(chainFile)
    val config = SslServerConfiguration(
      keyCredentials = goodKeyCredentials,
      trustCredentials = trustCredentials
    )
    val engine = factory(config)
    val sslEngine = engine.self

    assert(sslEngine != null)
  }

  test("config with bad trusted cert collection fails") {
    val tempCertFile = File.createTempFile("test", "crt")
    tempCertFile.deleteOnExit()

    val trustCredentials = TrustCredentials.CertCollection(tempCertFile)
    val config = SslServerConfiguration(
      keyCredentials = goodKeyCredentials,
      trustCredentials = trustCredentials
    )

    intercept[IllegalArgumentException] {
      val engine = factory(config)
    }
  }

  test("config with TrustManagerFactory and KeyManagerFactory succeeds") {
    val trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    trustManagerFactory.init(null.asInstanceOf[KeyStore])

    val trustCredentials = TrustCredentials.TrustManagerFactory(trustManagerFactory)

    val keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    keyManagerFactory.init(null, Array[Char]())

    val keyCredentials = KeyCredentials.KeyManagerFactory(keyManagerFactory)

    val config = SslServerConfiguration(trustCredentials = trustCredentials, keyCredentials = keyCredentials)
    val engine = factory(config)
    val sslEngine = engine.self

    assert(sslEngine != null)
  }

  test("config with only KeyCredentials succeeds") {
    val keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    keyManagerFactory.init(null, Array[Char]())

    val keyCredentials = KeyCredentials.KeyManagerFactory(keyManagerFactory)
    val config = SslServerConfiguration(keyCredentials = keyCredentials)
    val engine = factory(config)
    val sslEngine = engine.self

    assert(sslEngine != null)
  }


  test("config with good cipher suites succeeds") {
    val cipherSuites = CipherSuites.Enabled(Seq("TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384"))
    val config =
      SslServerConfiguration(keyCredentials = goodKeyCredentials, cipherSuites = cipherSuites)
    val engine = factory(config)
    val sslEngine = engine.self

    assert(sslEngine != null)
    val enabled = sslEngine.getEnabledCipherSuites()
    assert(enabled.length == 1)
    assert(enabled(0) == "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384")
  }

  test("config with bad cipher suites fails") {
    val cipherSuites = CipherSuites.Enabled(Seq("TLS_ECDHE_ECDSA_WITH_AES_102_CBC_SHA496"))
    val config =
      SslServerConfiguration(keyCredentials = goodKeyCredentials, cipherSuites = cipherSuites)

    intercept[IllegalArgumentException] {
      val engine = factory(config)
    }
  }

  test("config with good enabled protocols succeeds") {
    val protocols = Protocols.Enabled(Seq("TLSv1.2"))
    val config = SslServerConfiguration(keyCredentials = goodKeyCredentials, protocols = protocols)
    val engine = factory(config)
    val sslEngine = engine.self

    assert(sslEngine != null)
    val enabled = sslEngine.getEnabledProtocols()
    assert(enabled.length == 1)
    assert(enabled(0) == "TLSv1.2")
  }

  test("config with bad enabled protocols fails") {
    val protocols = Protocols.Enabled(Seq("TLSv2.0"))
    val config = SslServerConfiguration(keyCredentials = goodKeyCredentials, protocols = protocols)

    intercept[IllegalArgumentException] {
      val engine = factory(config)
    }
  }

  // application protocols are supported only by netty-tcnative, which is
  // not tested via these tests.
  test("config with any application protocols fails for JDK provider") {
    // tests are run against the JDK provider which does not support NPN_AND_ALPN
    val appProtocols = ApplicationProtocols.Supported(Seq("h2"))
    val config = SslServerConfiguration(
      keyCredentials = goodKeyCredentials,
      applicationProtocols = appProtocols
    )

    intercept[UnsupportedOperationException] {
      val engine = factory(config)
    }
  }

  test("config with client auth Off succeeds") {
    val config =
      SslServerConfiguration(keyCredentials = goodKeyCredentials, clientAuth = ClientAuth.Off)
    val engine = factory(config)
    val sslEngine = engine.self

    assert(sslEngine != null)
    assert(!sslEngine.getWantClientAuth())
    assert(!sslEngine.getNeedClientAuth())
  }

  test("config with client auth Wanted succeeds") {
    val config =
      SslServerConfiguration(keyCredentials = goodKeyCredentials, clientAuth = ClientAuth.Wanted)
    val engine = factory(config)
    val sslEngine = engine.self

    assert(sslEngine != null)
    assert(sslEngine.getWantClientAuth())
    assert(!sslEngine.getNeedClientAuth())
  }

  test("config with client auth Needed succeeds") {
    val config =
      SslServerConfiguration(keyCredentials = goodKeyCredentials, clientAuth = ClientAuth.Needed)
    val engine = factory(config)
    val sslEngine = engine.self

    assert(sslEngine != null)
    assert(!sslEngine.getWantClientAuth())
    assert(sslEngine.getNeedClientAuth())
  }
}
