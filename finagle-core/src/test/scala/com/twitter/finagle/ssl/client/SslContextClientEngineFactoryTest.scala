package com.twitter.finagle.ssl.client

import com.twitter.finagle.Address
import com.twitter.finagle.ssl._
import com.twitter.io.TempFile
import java.net.InetSocketAddress
import javax.net.ssl.SSLContext
import org.scalatest.funsuite.AnyFunSuite

class SslContextClientEngineFactoryTest extends AnyFunSuite {

  private[this] val address: Address = Address(new InetSocketAddress("localhost", 12345))
  private[this] val other: Address = Address.Failed(new Exception("testing"))

  private[this] val sslContext: SSLContext = {
    val result = SSLContext.getInstance("TLSv1.2")
    result.init(null, null, null)
    result
  }

  private[this] val factory = new SslContextClientEngineFactory(sslContext)

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

  test("config with any specified key credentials fails") {
    val tempCertFile = TempFile.fromResourcePath("/ssl/certs/test-rsa.crt")
    // deleteOnExit is handled by TempFile

    val tempKeyFile = TempFile.fromResourcePath("/ssl/keys/test-pkcs8.key")
    // deleteOnExit is handled by TempFile

    val keyCredentials = KeyCredentials.CertAndKey(tempCertFile, tempKeyFile)
    val config = SslClientConfiguration(keyCredentials = keyCredentials)

    intercept[SslConfigurationException] {
      factory(address, config)
    }
  }

  test("config with insecure trust credentials fails") {
    val config = SslClientConfiguration(trustCredentials = TrustCredentials.Insecure)

    intercept[SslConfigurationException] {
      factory(address, config)
    }
  }

  test("config with cert collection trust credentials fails") {
    val tempCertFile = TempFile.fromResourcePath("/ssl/certs/test-rsa.crt")
    // deleteOnExit is handled by TempFile

    val config =
      SslClientConfiguration(trustCredentials = TrustCredentials.CertCollection(tempCertFile))

    intercept[SslConfigurationException] {
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

  test("config with any application protocols fails") {
    val appProtocols = ApplicationProtocols.Supported(Seq("h2"))
    val config = SslClientConfiguration(applicationProtocols = appProtocols)

    intercept[SslConfigurationException] {
      factory(address, config)
    }
  }

}
