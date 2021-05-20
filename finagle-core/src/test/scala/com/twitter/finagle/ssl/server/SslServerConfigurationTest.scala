package com.twitter.finagle.ssl.server

import com.twitter.finagle.ssl.{
  ApplicationProtocols,
  CipherSuites,
  ClientAuth,
  KeyCredentials,
  Protocols,
  TrustCredentials
}
import java.io.File
import org.scalatest.funsuite.AnyFunSuite

class SslServerConfigurationTest extends AnyFunSuite {

  test("server configuration should contain non-destructive defaults") {
    val config = SslServerConfiguration()

    assert(config.keyCredentials == KeyCredentials.Unspecified)
    assert(config.clientAuth == ClientAuth.Unspecified)
    assert(config.trustCredentials == TrustCredentials.Unspecified)
    assert(config.cipherSuites == CipherSuites.Unspecified)
    assert(config.protocols == Protocols.Unspecified)
    assert(config.applicationProtocols == ApplicationProtocols.Unspecified)
  }

  test("key credentials can be set independently") {
    val certFile: File = File.createTempFile("test", "crt")
    certFile.deleteOnExit()

    val keyFile: File = File.createTempFile("test", "key")
    keyFile.deleteOnExit()

    val config =
      SslServerConfiguration(keyCredentials = KeyCredentials.CertAndKey(certFile, keyFile))

    config.keyCredentials match {
      case KeyCredentials.CertAndKey(testCertFile, testKeyFile) =>
        assert(testCertFile == certFile)
        assert(testKeyFile == keyFile)
      case _ => assert(false)
    }
  }

  test("client authentication can be set independently") {
    val config = SslServerConfiguration(clientAuth = ClientAuth.Needed)
    assert(config.clientAuth == ClientAuth.Needed)
  }

  test("trust credentials can be set independently") {
    val config = SslServerConfiguration(trustCredentials = TrustCredentials.Insecure)
    assert(config.trustCredentials == TrustCredentials.Insecure)
  }

  test("cipher suites can be set independently") {
    val suites = CipherSuites.fromString("made-up-cipher-suite")
    val config = SslServerConfiguration(cipherSuites = suites)
    assert(config.cipherSuites == CipherSuites.Enabled(Seq("made-up-cipher-suite")))
  }

  test("protocols can be set independently") {
    val protos = Protocols.Enabled(Seq("TLSv1.2", "SSLv3"))
    val config = SslServerConfiguration(protocols = protos)
    assert(config.protocols == Protocols.Enabled(Seq("TLSv1.2", "SSLv3")))
  }

  test("application protocols can be set independently") {
    val appProtos = ApplicationProtocols.Supported(Seq("h2"))
    val config = SslServerConfiguration(applicationProtocols = appProtos)
    assert(config.applicationProtocols == ApplicationProtocols.Supported(Seq("h2")))
  }

}
