package com.twitter.finagle.ssl.server

import com.twitter.finagle.ssl._
import com.twitter.io.TempFile
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LegacyServerEngineFactoryTest extends FunSuite {

  // LegacyServerEngineFactory uses the legacy `Ssl`
  // object, which first attempts to use the `OpenSSL`
  // engine, and then falls back to the `JSSE` engine.
  //
  // For the `OpenSSL` engine to work, it requires
  // Finagle-Native (patched tomcat-native), which
  // is not appropriate to depend on for these tests.
  //
  // For the `JSSE` engine to work, it requires a
  // call to `PEMEncodedKeyManager` which shells out
  // to command line OpenSSL, which is not appropriate
  // to depend on for these tests either.
  //
  // So, these tests attempt to cover the items which
  // we know are safe to test and should work universally.
  // Other items have been manually verified.

  test("default config fails") {
    val config = SslServerConfiguration()

    intercept[SslConfigurationException] {
      val engine = LegacyServerEngineFactory(config)
    }
  }

  test("config with insecure trust credentials fails") {
    val trustCredentials = TrustCredentials.Insecure
    val config = SslServerConfiguration(trustCredentials = trustCredentials)

    intercept[SslConfigurationException] {
      val engine = LegacyServerEngineFactory(config)
    }
  }

  test("config with good trusted cert collection fails") {
    val tempCertFile = TempFile.fromResourcePath("/ssl/certs/test-rsa.crt")
    // deleteOnExit is handled by TempFile

    val trustCredentials = TrustCredentials.CertCollection(tempCertFile)
    val config = SslServerConfiguration(trustCredentials = trustCredentials)

    intercept[SslConfigurationException] {
      val engine = LegacyServerEngineFactory(config)
    }
  }

  test("config with any enabled protocols fails") {
    val protocols = Protocols.Enabled(Seq("TLSv1.2"))
    val config = SslServerConfiguration(protocols = protocols)

    intercept[SslConfigurationException] {
      val engine = LegacyServerEngineFactory(config)
    }
  }

  test("config with client auth Off fails") {
    val clientAuth = ClientAuth.Off
    val config = SslServerConfiguration(clientAuth = clientAuth)
    
    intercept[SslConfigurationException] {
      val engine = LegacyServerEngineFactory(config)
    }
  }

  test("config with client auth Wanted fails") {
    val clientAuth = ClientAuth.Wanted
    val config = SslServerConfiguration(clientAuth = clientAuth)
    
    intercept[SslConfigurationException] {
      val engine = LegacyServerEngineFactory(config)
    }
  }

  test("config with client auth Needed fails") {
    val clientAuth = ClientAuth.Needed
    val config = SslServerConfiguration(clientAuth = clientAuth)
    
    intercept[SslConfigurationException] {
      val engine = LegacyServerEngineFactory(config)
    }
  }

}
