package com.twitter.finagle.netty4.ssl.client

import com.twitter.finagle.Address
import com.twitter.finagle.ssl._
import com.twitter.finagle.ssl.client.SslClientConfiguration
import com.twitter.io.TempFile
import java.io.File
import java.net.InetSocketAddress
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Netty4ClientEngineFactoryTest extends FunSuite {

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
    val tempCertFile = TempFile.fromResourcePath("/ssl/certs/test-rsa.crt")
    // deleteOnExit is handled by TempFile

    val tempKeyFile = TempFile.fromResourcePath("/ssl/keys/test-pkcs8.key")
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

    val tempKeyFile = TempFile.fromResourcePath("/ssl/keys/test-pkcs8.key")
    // deleteOnExit is handled by TempFile

    val keyCredentials = KeyCredentials.CertAndKey(tempCertFile, tempKeyFile)
    val config = SslClientConfiguration(keyCredentials = keyCredentials)

    intercept[IllegalArgumentException] {
      val engine = factory(address, config)
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
      val engine = factory(address, config)
    }
  }

  test("config with insecure trust credentials succeeds") {
    val config = SslClientConfiguration(trustCredentials = TrustCredentials.Insecure)
    val engine = factory(address, config)
    val sslEngine = engine.self

    assert(sslEngine != null)
  }

  test("config with good trusted cert collection succeeds") {
    val tempCertFile = TempFile.fromResourcePath("/ssl/certs/test-rsa.crt")
    // deleteOnExit is handled by TempFile

    val trustCredentials = TrustCredentials.CertCollection(tempCertFile)
    val config = SslClientConfiguration(trustCredentials = trustCredentials)
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
      val engine = factory(address, config)
    }
  }

  test("config with good cipher suites succeeds") {
    val cipherSuites = CipherSuites.Enabled(Seq("TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384"))
    val config = SslClientConfiguration(cipherSuites = cipherSuites)
    val engine = factory(address, config)
    val sslEngine = engine.self

    assert(sslEngine != null)
    val enabled = sslEngine.getEnabledCipherSuites()
    assert(enabled.length == 1)
    assert(enabled(0) == "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384")
  }

  test("config with bad cipher suites fails") {
    val cipherSuites = CipherSuites.Enabled(Seq("TLS_ECDHE_ECDSA_WITH_AES_102_CBC_SHA496"))
    val config = SslClientConfiguration(cipherSuites = cipherSuites)

    intercept[IllegalArgumentException] {
      val engine = factory(address, config)
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
      val engine = factory(address, config)
    }
  }

  // application protocols are supported only by netty-tcnative, which is
  // not tested via these tests.
  test("config with any application protocols fails for JDK provider") {
    // tests are run against the JDK provider which does not support NPN_AND_ALPN
    val appProtocols = ApplicationProtocols.Supported(Seq("h2"))
    val config = SslClientConfiguration(applicationProtocols = appProtocols)

    intercept[UnsupportedOperationException] {
      val engine = factory(address, config)
    }
  }

}

