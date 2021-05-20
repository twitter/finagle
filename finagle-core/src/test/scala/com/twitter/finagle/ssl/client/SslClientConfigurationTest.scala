package com.twitter.finagle.ssl.client

import com.twitter.finagle.ssl.{
  ApplicationProtocols,
  CipherSuites,
  KeyCredentials,
  Protocols,
  TrustCredentials
}
import java.io.File
import org.scalatest.funsuite.AnyFunSuite

class SslClientConfigurationTest extends AnyFunSuite {

  test("client configuration should contain non-destructive defaults") {
    val config = SslClientConfiguration()

    assert(config.hostname.isEmpty)
    assert(config.keyCredentials == KeyCredentials.Unspecified)
    assert(config.trustCredentials == TrustCredentials.Unspecified)
    assert(config.cipherSuites == CipherSuites.Unspecified)
    assert(config.protocols == Protocols.Unspecified)
    assert(config.applicationProtocols == ApplicationProtocols.Unspecified)
  }

  test("hostname for certificate validation can be set independently") {
    val config = SslClientConfiguration(hostname = Some("twitter.com"))

    assert(config.hostname.exists(_ == "twitter.com"))
  }

  test("key credentials can be set independently") {
    val certFile: File = File.createTempFile("test", "crt")
    certFile.deleteOnExit()

    val keyFile: File = File.createTempFile("test", "key")
    keyFile.deleteOnExit()

    val config =
      SslClientConfiguration(keyCredentials = KeyCredentials.CertAndKey(certFile, keyFile))

    config.keyCredentials match {
      case KeyCredentials.CertAndKey(testCertFile, testKeyFile) =>
        assert(testCertFile == certFile)
        assert(testKeyFile == keyFile)
      case _ => fail()
    }
  }

  test("trust credentials can be set independently") {
    val config = SslClientConfiguration(trustCredentials = TrustCredentials.Insecure)
    assert(config.trustCredentials == TrustCredentials.Insecure)
  }

  test("cipher suites can be set independently") {
    val suites = CipherSuites.fromString("made-up-cipher-suite")
    val config = SslClientConfiguration(cipherSuites = suites)
    assert(config.cipherSuites == CipherSuites.Enabled(Seq("made-up-cipher-suite")))
  }

  test("protocols can be set independently") {
    val protos = Protocols.Enabled(Seq("TLSv1.2", "SSLv3"))
    val config = SslClientConfiguration(protocols = protos)
    assert(config.protocols == Protocols.Enabled(Seq("TLSv1.2", "SSLv3")))
  }

  test("application protocols can be set independently") {
    val appProtos = ApplicationProtocols.Supported(Seq("h2"))
    val config = SslClientConfiguration(applicationProtocols = appProtos)
    assert(config.applicationProtocols == ApplicationProtocols.Supported(Seq("h2")))
  }

}
