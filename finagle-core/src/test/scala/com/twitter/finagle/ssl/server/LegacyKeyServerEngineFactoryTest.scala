package com.twitter.finagle.ssl.server

import com.twitter.finagle.ssl._
import com.twitter.io.TempFile
import java.io.File
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LegacyKeyServerEngineFactoryTest extends FunSuite {

  test("default config succeeds") {
    val config = SslServerConfiguration()
    val engine = LegacyKeyServerEngineFactory(config)
    val sslEngine = engine.self
    assert(sslEngine != null)
    assert(!sslEngine.getUseClientMode())
  }

  test("config with good cert and key credentials succeeds") {
    val tempCertFile = TempFile.fromResourcePath("/ssl/certs/test-rsa.crt")
    // deleteOnExit is handled by TempFile

    val tempKeyFile = TempFile.fromResourcePath("/ssl/keys/test-pkcs1.key")
    // deleteOnExit is handled by TempFile

    val keyCredentials = KeyCredentials.CertAndKey(tempCertFile, tempKeyFile)
    val config = SslServerConfiguration(keyCredentials = keyCredentials)
    val engine = LegacyKeyServerEngineFactory(config)
    val sslEngine = engine.self

    assert(sslEngine != null)
  }

  test("config with bad cert of key credential fails") {
    val tempCertFile = File.createTempFile("test", "crt")
    tempCertFile.deleteOnExit()

    val tempKeyFile = TempFile.fromResourcePath("/ssl/keys/test-pkcs1.key")
    // deleteOnExit is handled by TempFile

    val keyCredentials = KeyCredentials.CertAndKey(tempCertFile, tempKeyFile)
    val config = SslServerConfiguration(keyCredentials = keyCredentials)

    intercept[SslConfigurationException] {
      val engine = LegacyKeyServerEngineFactory(config)
    }
  }

  test("config with cert, key, and chain succeeds") {
    val tempCertFile = File.createTempFile("test", "crt")
    tempCertFile.deleteOnExit()

    val tempKeyFile = TempFile.fromResourcePath("/ssl/keys/test-pkcs1.key")
    // deleteOnExit is handled by TempFile

    val tempChainFile = TempFile.fromResourcePath("/ssl/certs/test-rsa-chain.crt")
    // deleteOnExit is handled by TempFile

    val keyCredentials = KeyCredentials.CertKeyAndChain(tempCertFile, tempKeyFile, tempChainFile)
    val config = SslServerConfiguration(keyCredentials = keyCredentials)
    val engine = LegacyKeyServerEngineFactory(config)
    val sslEngine = engine.self

    assert(sslEngine != null)
  }

  test("config with bad cert, key, and chain fails") {
    val tempCertFile = File.createTempFile("test", "crt")
    tempCertFile.deleteOnExit()

    val tempKeyFile = TempFile.fromResourcePath("/ssl/keys/test-pkcs1.key")
    // deleteOnExit is handled by TempFile

    val tempChainFile = File.createTempFile("test-chain", "crt")
    tempChainFile.deleteOnExit()

    val keyCredentials = KeyCredentials.CertKeyAndChain(tempCertFile, tempKeyFile, tempChainFile)
    val config = SslServerConfiguration(keyCredentials = keyCredentials)

    intercept[SslConfigurationException] {
      val engine = LegacyKeyServerEngineFactory(config)
    }
  }

  test("config with insecure trust credentials succeeds") {
    val config = SslServerConfiguration(trustCredentials = TrustCredentials.Insecure)
    val engine = LegacyKeyServerEngineFactory(config)
    val sslEngine = engine.self

    assert(sslEngine != null)
  }

  test("config with good trusted cert collection succeeds") {
    val tempCertFile = TempFile.fromResourcePath("/ssl/certs/test-rsa.crt")
    // deleteOnExit is handled by TempFile

    val trustCredentials = TrustCredentials.CertCollection(tempCertFile)
    val config = SslServerConfiguration(trustCredentials = trustCredentials)
    val engine = LegacyKeyServerEngineFactory(config)
    val sslEngine = engine.self

    assert(sslEngine != null)
  }

  test("config with bad trusted cert collection fails") {
    val tempCertFile = File.createTempFile("test", "crt")
    tempCertFile.deleteOnExit()

    val trustCredentials = TrustCredentials.CertCollection(tempCertFile)
    val config = SslServerConfiguration(trustCredentials = trustCredentials)

    intercept[SslConfigurationException] {
      val engine = LegacyKeyServerEngineFactory(config)
    }
  }

  test("config with good cipher suites succeeds") {
    val cipherSuites = CipherSuites.Enabled(Seq("TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384"))
    val config = SslServerConfiguration(cipherSuites = cipherSuites)
    val engine = LegacyKeyServerEngineFactory(config)
    val sslEngine = engine.self

    assert(sslEngine != null)
    val enabled = sslEngine.getEnabledCipherSuites()
    assert(enabled.length == 1)
    assert(enabled(0) == "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384")
  }

  test("config with bad cipher suites fails") {
    val cipherSuites = CipherSuites.Enabled(Seq("TLS_ECDHE_ECDSA_WITH_AES_102_CBC_SHA496"))
    val config = SslServerConfiguration(cipherSuites = cipherSuites)

    intercept[IllegalArgumentException] {
      val engine = LegacyKeyServerEngineFactory(config)
    }
  }

  test("config with good enabled protocols succeeds") {
    val protocols = Protocols.Enabled(Seq("TLSv1.2"))
    val config = SslServerConfiguration(protocols = protocols)
    val engine = LegacyKeyServerEngineFactory(config)
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
      val engine = LegacyKeyServerEngineFactory(config)
    }
  }

  test("config with any application protocols fails") {
    val appProtocols = ApplicationProtocols.Supported(Seq("h2"))
    val config = SslServerConfiguration(applicationProtocols = appProtocols)

    intercept[SslConfigurationException] {
      val engine = LegacyKeyServerEngineFactory(config)
    }
  }

  test("config with client auth Off succeeds") {
    val config = SslServerConfiguration(clientAuth = ClientAuth.Off)
    val engine = LegacyKeyServerEngineFactory(config)
    val sslEngine = engine.self

    assert(sslEngine != null)
    assert(!sslEngine.getWantClientAuth())
    assert(!sslEngine.getNeedClientAuth())
  }

  test("config with client auth Wanted succeeds") {
    val config = SslServerConfiguration(clientAuth = ClientAuth.Wanted)
    val engine = LegacyKeyServerEngineFactory(config)
    val sslEngine = engine.self

    assert(sslEngine != null)
    assert(sslEngine.getWantClientAuth())
    assert(!sslEngine.getNeedClientAuth())
  }

  test("config with client auth Needed succeeds") {
    val config = SslServerConfiguration(clientAuth = ClientAuth.Needed)
    val engine = LegacyKeyServerEngineFactory(config)
    val sslEngine = engine.self

    assert(sslEngine != null)
    assert(!sslEngine.getWantClientAuth())
    assert(sslEngine.getNeedClientAuth())
  }
}
