package com.twitter.finagle.netty4.ssl.client

import com.twitter.finagle.Address
import com.twitter.finagle.ssl._
import com.twitter.finagle.ssl.client.SslClientConfiguration
import com.twitter.io.TempFile
import java.io.File
import java.net.InetSocketAddress
import java.security.KeyStore
import javax.net.ssl.{KeyManagerFactory, TrustManagerFactory}
import org.scalatest.funsuite.AnyFunSuite

class Netty4ClientEngineFactoryTest extends AnyFunSuite {

  private[this] val address: Address = Address(new InetSocketAddress("localhost", 12345))
  private[this] val other: Address = Address.Failed(new Exception("testing"))

  // Force JDK version for tests, because the native engine could fail to load in different
  // environments
  private[this] val factory = Netty4ClientEngineFactory(forceJdk = true)

  test("default config with inet address creates client engine with peer") {
    val config = SslClientConfiguration()
    val engine = factory(address, config)
    val sslEngine = engine.self

    assert(sslEngine.getUseClientMode())
    assert(sslEngine.getPeerHost() == "localhost")
    assert(sslEngine.getPeerPort() == 12345)
  }

  test("default config without inet address creates client engine without peer") {
    val config = SslClientConfiguration()
    val engine = factory(other, config)
    val sslEngine = engine.self

    assert(sslEngine.getUseClientMode())
    assert(sslEngine.getPeerHost() == null)
    assert(sslEngine.getPeerPort() == -1)
  }

  test("config with good cert and key credentials succeeds") {
    val tempCertFile = TempFile.fromResourcePath("/ssl/certs/svc-test-client.cert.pem")
    // deleteOnExit is handled by TempFile

    val tempKeyFile = TempFile.fromResourcePath("/ssl/keys/svc-test-client-pkcs8.key.pem")
    // deleteOnExit is handled by TempFile

    val keyCredentials = KeyCredentials.CertAndKey(tempCertFile, tempKeyFile)
    val config = SslClientConfiguration(keyCredentials = keyCredentials)
    val engine = factory(address, config)
    val sslEngine = engine.self

    assert(sslEngine != null)
  }

  test("config with bad cert or key credential fails") {
    val tempCertFile = File.createTempFile("test", "crt")
    tempCertFile.deleteOnExit()

    val tempKeyFile = TempFile.fromResourcePath("/ssl/keys/svc-test-client-pkcs8.key.pem")
    // deleteOnExit is handled by TempFile

    val keyCredentials = KeyCredentials.CertAndKey(tempCertFile, tempKeyFile)
    val config = SslClientConfiguration(keyCredentials = keyCredentials)

    intercept[SslConfigurationException] {
      factory(address, config)
    }
  }

  test("config with good cert chain and key credentials succeeds") {
    val tempCertFile = TempFile.fromResourcePath("/ssl/certs/svc-test-client-full-chain.cert.pem")
    // deleteOnExit is handled by TempFile

    val tempKeyFile = TempFile.fromResourcePath("/ssl/keys/svc-test-client-pkcs8.key.pem")
    // deleteOnExit is handled by TempFile

    val keyCredentials = KeyCredentials.CertsAndKey(tempCertFile, tempKeyFile)
    val config = SslClientConfiguration(keyCredentials = keyCredentials)
    val engine = factory(address, config)
    val sslEngine = engine.self

    assert(sslEngine != null)
  }

  test("config with bad cert chain or key credential fails") {
    val tempCertFile = File.createTempFile("test", "crt")
    tempCertFile.deleteOnExit()

    val tempKeyFile = TempFile.fromResourcePath("/ssl/keys/svc-test-client-pkcs8.key.pem")
    // deleteOnExit is handled by TempFile

    val keyCredentials = KeyCredentials.CertsAndKey(tempCertFile, tempKeyFile)
    val config = SslClientConfiguration(keyCredentials = keyCredentials)

    intercept[SslConfigurationException] {
      factory(address, config)
    }
  }

  test("config with expired cert and valid key credential fails") {
    val tempCertFile = TempFile.fromResourcePath("/ssl/certs/svc-test-client-expired.cert.pem")
    // deleteOnExit is handled by TempFile

    val tempKeyFile = TempFile.fromResourcePath("/ssl/keys/svc-test-client-pkcs8.key.pem")
    // deleteOnExit is handled by TempFile

    val keyCredentials = KeyCredentials.CertAndKey(tempCertFile, tempKeyFile)
    val config = SslClientConfiguration(keyCredentials = keyCredentials)

    intercept[SslConfigurationException] {
      factory(address, config)
    }
  }

  test("config with cert, key, and chain succeeds") {
    val tempCertFile = TempFile.fromResourcePath("/ssl/certs/svc-test-client.cert.pem")
    // deleteOnExit is handled by TempFile

    val tempKeyFile = TempFile.fromResourcePath("/ssl/keys/svc-test-client-pkcs8.key.pem")
    // deleteOnExit is handled by TempFile

    // This file contains multiple certificates
    val tempChainFile = TempFile.fromResourcePath("/ssl/certs/svc-test-chain.cert.pem")
    // deleteOnExit is handled by TempFile

    val keyCredentials = KeyCredentials.CertKeyAndChain(tempCertFile, tempKeyFile, tempChainFile)
    val config = SslClientConfiguration(keyCredentials = keyCredentials)

    val engine = factory(address, config)
    val sslEngine = engine.self

    assert(sslEngine != null)
  }

  test("config with insecure trust credentials succeeds") {
    val config = SslClientConfiguration(trustCredentials = TrustCredentials.Insecure)
    val engine = factory(address, config)
    val sslEngine = engine.self

    assert(sslEngine != null)
  }

  test("config with good trusted cert collection succeeds") {
    val tempCertFile = TempFile.fromResourcePath("/ssl/certs/svc-test-chain.cert.pem")
    // deleteOnExit is handled by TempFile

    val trustCredentials = TrustCredentials.CertCollection(tempCertFile)
    val config = SslClientConfiguration(trustCredentials = trustCredentials)
    val engine = factory(address, config)
    val sslEngine = engine.self

    assert(sslEngine != null)
  }

  test("config with TrustManagerFactory succeeds") {
    val trustManagerFactory =
      TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    trustManagerFactory.init(null.asInstanceOf[KeyStore])

    val trustCredentials = TrustCredentials.TrustManagerFactory(trustManagerFactory)
    val config = SslClientConfiguration(trustCredentials = trustCredentials)
    val engine = factory(address, config)
    val sslEngine = engine.self

    assert(sslEngine != null)
  }

  test("config with KeyManagerFactory succeeds") {
    val keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    keyManagerFactory.init(null, Array[Char]())

    val keyCredentials = KeyCredentials.KeyManagerFactory(keyManagerFactory)
    val config = SslClientConfiguration(keyCredentials = keyCredentials)
    val engine = factory(address, config)
    val sslEngine = engine.self

    assert(sslEngine != null)
  }

  test("config with bad trusted cert collection fails") {
    val tempCertFile = File.createTempFile("test", "crt")
    tempCertFile.deleteOnExit()

    val trustCredentials = TrustCredentials.CertCollection(tempCertFile)
    val config = SslClientConfiguration(trustCredentials = trustCredentials)

    intercept[IllegalArgumentException] {
      factory(address, config)
    }
  }

  test("config with good cipher suites succeeds") {
    val cipherSuites = CipherSuites.Enabled(Seq("TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"))
    val config = SslClientConfiguration(cipherSuites = cipherSuites)
    val engine = factory(address, config)
    val sslEngine = engine.self

    assert(sslEngine != null)
    val enabled = sslEngine.getEnabledCipherSuites()
    assert(enabled.length == 1)
    assert(enabled(0) == "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256")
  }

  test("config with bad cipher suites fails") {
    val cipherSuites = CipherSuites.Enabled(Seq("TLS_ECDHE_ECDSA_WITH_AES_102_CBC_SHA496"))
    val config = SslClientConfiguration(cipherSuites = cipherSuites)

    intercept[IllegalArgumentException] {
      factory(address, config)
    }
  }

  test("config with good enabled protocols succeeds") {
    val protocols = Protocols.Enabled(Seq("TLSv1.2"))
    val config = SslClientConfiguration(protocols = protocols)
    val engine = factory(address, config)
    val sslEngine = engine.self

    assert(sslEngine != null)
    val enabled = sslEngine.getEnabledProtocols()
    assert(enabled.length == 1)
    assert(enabled(0) == "TLSv1.2")
  }

  test("config with bad enabled protocols fails") {
    val protocols = Protocols.Enabled(Seq("TLSv2.0"))
    val config = SslClientConfiguration(protocols = protocols)

    intercept[IllegalArgumentException] {
      factory(address, config)
    }
  }
}
