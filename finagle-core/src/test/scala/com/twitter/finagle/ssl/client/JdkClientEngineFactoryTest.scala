package com.twitter.finagle.ssl.client

import com.twitter.finagle.Address
import com.twitter.finagle.ssl._
import com.twitter.io.TempFile
import java.io.File
import java.net.InetSocketAddress
import org.scalatest.funsuite.AnyFunSuite

class JdkClientEngineFactoryTest extends AnyFunSuite {

  private[this] val address: Address = Address(new InetSocketAddress("localhost", 12345))
  private[this] val other: Address = Address.Failed(new Exception("testing"))

  test("default config with inet address creates client engine with peer") {
    val config = SslClientConfiguration()
    val engine = JdkClientEngineFactory(address, config)
    val sslEngine = engine.self

    assert(sslEngine.getUseClientMode())
    assert(sslEngine.getPeerHost() == "localhost")
    assert(sslEngine.getPeerPort() == 12345)
  }

  test("default config without inet address creates client engine without peer") {
    val config = SslClientConfiguration()
    val engine = JdkClientEngineFactory(other, config)
    val sslEngine = engine.self

    assert(sslEngine.getUseClientMode())
    assert(sslEngine.getPeerHost() == null)
    assert(sslEngine.getPeerPort() == -1)
  }

  test("config with good cert and key credentials succeeds") {
    val tempCertFile = TempFile.fromResourcePath("/ssl/certs/test-rsa.crt")
    // deleteOnExit is handled by TempFile

    val tempKeyFile = TempFile.fromResourcePath("/ssl/keys/test-pkcs8.key")
    // deleteOnExit is handled by TempFile

    val keyCredentials = KeyCredentials.CertAndKey(tempCertFile, tempKeyFile)
    val config = SslClientConfiguration(keyCredentials = keyCredentials)
    val engine = JdkClientEngineFactory(address, config)
    val sslEngine = engine.self

    assert(sslEngine != null)
  }

  test("config with bad cert or key credential fails") {
    val tempCertFile = File.createTempFile("test", "crt")
    tempCertFile.deleteOnExit()

    val tempKeyFile = TempFile.fromResourcePath("/ssl/keys/test-pkcs8.key")
    // deleteOnExit is handled by TempFile

    val keyCredentials = KeyCredentials.CertAndKey(tempCertFile, tempKeyFile)
    val config = SslClientConfiguration(keyCredentials = keyCredentials)

    intercept[SslConfigurationException] {
      JdkClientEngineFactory(address, config)
    }
  }

  test("config with good cert chain and key credentials succeeds") {
    val tempCertFile = TempFile.fromResourcePath("/ssl/certs/test-rsa-full-cert-chain.crt")
    // deleteOnExit is handled by TempFile

    val tempKeyFile = TempFile.fromResourcePath("/ssl/keys/test-pkcs8.key")
    // deleteOnExit is handled by TempFile

    val keyCredentials = KeyCredentials.CertsAndKey(tempCertFile, tempKeyFile)
    val config = SslClientConfiguration(keyCredentials = keyCredentials)
    val engine = JdkClientEngineFactory(address, config)
    val sslEngine = engine.self

    assert(sslEngine != null)
  }

  test("config with bad cert chain or key credential fails") {
    val tempCertFile = File.createTempFile("test", "crt")
    tempCertFile.deleteOnExit()

    val tempKeyFile = TempFile.fromResourcePath("/ssl/keys/test-pkcs8.key")
    // deleteOnExit is handled by TempFile

    val keyCredentials = KeyCredentials.CertsAndKey(tempCertFile, tempKeyFile)
    val config = SslClientConfiguration(keyCredentials = keyCredentials)

    intercept[SslConfigurationException] {
      JdkClientEngineFactory(address, config)
    }
  }

  test("config with cert, key, and chain fails") {
    val tempCertFile = TempFile.fromResourcePath("/ssl/certs/test-rsa.crt")
    // deleteOnExit is handled by TempFile

    val tempKeyFile = TempFile.fromResourcePath("/ssl/keys/test-pkcs8.key")
    // deleteOnExit is handled by TempFile

    val keyCredentials = KeyCredentials.CertKeyAndChain(tempCertFile, tempKeyFile, tempCertFile)
    val config = SslClientConfiguration(keyCredentials = keyCredentials)

    intercept[SslConfigurationException] {
      JdkClientEngineFactory(address, config)
    }
  }

  test("config with insecure trust credentials succeeds") {
    val config = SslClientConfiguration(trustCredentials = TrustCredentials.Insecure)
    val engine = JdkClientEngineFactory(address, config)
    val sslEngine = engine.self

    assert(sslEngine != null)
  }

  test("config with good trusted cert collection succeeds") {
    val tempCertFile = TempFile.fromResourcePath("/ssl/certs/test-rsa.crt")
    // deleteOnExit is handled by TempFile

    val trustCredentials = TrustCredentials.CertCollection(tempCertFile)
    val config = SslClientConfiguration(trustCredentials = trustCredentials)
    val engine = JdkClientEngineFactory(address, config)
    val sslEngine = engine.self

    assert(sslEngine != null)
  }

  test("config with bad trusted cert collection fails") {
    val tempCertFile = File.createTempFile("test", "crt")
    tempCertFile.deleteOnExit()

    val trustCredentials = TrustCredentials.CertCollection(tempCertFile)
    val config = SslClientConfiguration(trustCredentials = trustCredentials)

    intercept[SslConfigurationException] {
      JdkClientEngineFactory(address, config)
    }
  }

  test("config with good cipher suites succeeds") {
    val cipherSuites = CipherSuites.Enabled(Seq("TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"))
    val config = SslClientConfiguration(cipherSuites = cipherSuites)
    val engine = JdkClientEngineFactory(address, config)
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
      JdkClientEngineFactory(address, config)
    }
  }

  test("config with good enabled protocols succeeds") {
    val protocols = Protocols.Enabled(Seq("TLSv1.2"))
    val config = SslClientConfiguration(protocols = protocols)
    val engine = JdkClientEngineFactory(address, config)
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
      JdkClientEngineFactory(address, config)
    }
  }

  test("config with any application protocols fails") {
    val appProtocols = ApplicationProtocols.Supported(Seq("h2"))
    val config = SslClientConfiguration(applicationProtocols = appProtocols)

    intercept[SslConfigurationException] {
      JdkClientEngineFactory(address, config)
    }
  }

}
