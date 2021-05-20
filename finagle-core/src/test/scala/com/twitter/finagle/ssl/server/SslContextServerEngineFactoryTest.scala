package com.twitter.finagle.ssl.server

import com.twitter.finagle.ssl._
import com.twitter.io.TempFile
import javax.net.ssl.SSLContext
import org.scalatest.funsuite.AnyFunSuite

class SslContextServerEngineFactoryTest extends AnyFunSuite {

  private[this] val sslContext: SSLContext = {
    val result = SSLContext.getInstance("TLSv1.2")
    result.init(null, null, null)
    result
  }

  private[this] val factory = new SslContextServerEngineFactory(sslContext)

  test("default config succeeds") {
    val config = SslServerConfiguration()
    val engine = factory(config)
    val sslEngine = engine.self
    assert(sslEngine != null)
    assert(!sslEngine.getUseClientMode())
  }

  test("config with any specified key credentials fails") {
    val tempCertFile = TempFile.fromResourcePath("/ssl/certs/test-rsa.crt")
    // deleteOnExit is handled by TempFile

    val tempKeyFile = TempFile.fromResourcePath("/ssl/keys/test-pkcs8.key")
    // deleteOnExit is handled by TempFile

    val keyCredentials = KeyCredentials.CertAndKey(tempCertFile, tempKeyFile)
    val config = SslServerConfiguration(keyCredentials = keyCredentials)

    intercept[SslConfigurationException] {
      factory(config)
    }
  }

  test("config with certs and key fails") {
    val tempCertsFile = TempFile.fromResourcePath("/ssl/certs/test-rsa-full-cert-chain.crt")
    // deleteOnExit is handled by TempFile

    val tempKeyFile = TempFile.fromResourcePath("/ssl/keys/test-pkcs8.key")
    // deleteOnExit is handled by TempFile

    val keyCredentials = KeyCredentials.CertsAndKey(tempCertsFile, tempKeyFile)
    val config = SslServerConfiguration(keyCredentials = keyCredentials)

    intercept[SslConfigurationException] {
      factory(config)
    }

  }

  test("config with cert, key, and chain fails") {
    val tempCertFile = TempFile.fromResourcePath("/ssl/certs/test-rsa.crt")
    // deleteOnExit is handled by TempFile

    val tempKeyFile = TempFile.fromResourcePath("/ssl/keys/test-pkcs8.key")
    // deleteOnExit is handled by TempFile

    val keyCredentials = KeyCredentials.CertKeyAndChain(tempCertFile, tempKeyFile, tempCertFile)
    val config = SslServerConfiguration(keyCredentials = keyCredentials)

    intercept[SslConfigurationException] {
      factory(config)
    }
  }

  test("config with insecure trust credentials fails") {
    val config = SslServerConfiguration(trustCredentials = TrustCredentials.Insecure)

    intercept[SslConfigurationException] {
      factory(config)
    }
  }

  test("config with cert collection trust credentials fails") {
    val tempCertFile = TempFile.fromResourcePath("/ssl/certs/test-rsa.crt")
    // deleteOnExit is handled by TempFile

    val trustCredentials = TrustCredentials.CertCollection(tempCertFile)
    val config = SslServerConfiguration(trustCredentials = trustCredentials)

    intercept[SslConfigurationException] {
      factory(config)
    }
  }

  test("config with good cipher suites succeeds") {
    val cipherSuites = CipherSuites.Enabled(Seq("TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"))
    val config = SslServerConfiguration(cipherSuites = cipherSuites)
    val engine = factory(config)
    val sslEngine = engine.self

    assert(sslEngine != null)
    val enabled = sslEngine.getEnabledCipherSuites()
    assert(enabled.length == 1)
    assert(enabled(0) == "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256")
  }

  test("config with bad cipher suites fails") {
    val cipherSuites = CipherSuites.Enabled(Seq("TLS_ECDHE_ECDSA_WITH_AES_102_CBC_SHA496"))
    val config = SslServerConfiguration(cipherSuites = cipherSuites)

    intercept[IllegalArgumentException] {
      factory(config)
    }
  }

  test("config with good enabled protocols succeeds") {
    val protocols = Protocols.Enabled(Seq("TLSv1.2"))
    val config = SslServerConfiguration(protocols = protocols)
    val engine = factory(config)
    val sslEngine = engine.self

    assert(sslEngine != null)
    val enabled = sslEngine.getEnabledProtocols()
    assert(enabled.length == 1)
    assert(enabled(0) == "TLSv1.2")
  }

  test("config with bad enabled protocols fails") {
    val protocols = Protocols.Enabled(Seq("TLSv2.0"))
    val config = SslServerConfiguration(protocols = protocols)

    intercept[IllegalArgumentException] {
      factory(config)
    }
  }

  test("config with any application protocols fails") {
    val appProtocols = ApplicationProtocols.Supported(Seq("h2"))
    val config = SslServerConfiguration(applicationProtocols = appProtocols)

    intercept[SslConfigurationException] {
      factory(config)
    }
  }

  test("config with client auth Off succeeds") {
    val config = SslServerConfiguration(clientAuth = ClientAuth.Off)
    val engine = factory(config)
    val sslEngine = engine.self

    assert(sslEngine != null)
    assert(!sslEngine.getWantClientAuth())
    assert(!sslEngine.getNeedClientAuth())
  }

  test("config with client auth Wanted succeeds") {
    val config = SslServerConfiguration(clientAuth = ClientAuth.Wanted)
    val engine = factory(config)
    val sslEngine = engine.self

    assert(sslEngine != null)
    assert(sslEngine.getWantClientAuth())
    assert(!sslEngine.getNeedClientAuth())
  }

  test("config with client auth Needed succeeds") {
    val config = SslServerConfiguration(clientAuth = ClientAuth.Needed)
    val engine = factory(config)
    val sslEngine = engine.self

    assert(sslEngine != null)
    assert(!sslEngine.getWantClientAuth())
    assert(sslEngine.getNeedClientAuth())
  }
}
