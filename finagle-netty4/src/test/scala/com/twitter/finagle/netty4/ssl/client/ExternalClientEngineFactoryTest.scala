package com.twitter.finagle.netty4.ssl.client

import com.twitter.finagle.Address
import com.twitter.finagle.ssl.{
  ApplicationProtocols,
  Engine,
  SslConfigurationException,
  TrustCredentials
}
import com.twitter.finagle.ssl.client.{SslClientConfiguration, SslClientEngineFactory}
import com.twitter.io.TempFile
import java.net.InetSocketAddress
import org.scalatest.funsuite.AnyFunSuite

class ExternalClientEngineFactoryTest extends AnyFunSuite {

  // Fully testing the `ExternalClientEngineFactory` would require
  // connecting to remote hosts and verifying whether the TLS
  // connections work correctly. That is not allowed within our
  // JUnit security environment.
  //
  // So, verify that the four known use cases are 'capable' of
  // creating an engine.

  private[this] val tempCertsFile =
    TempFile.fromResourcePath("/ssl/certs/svc-test-chain.cert.pem")
  // deleteOnExit is handled by TempFile
  private[this] val certCollection: TrustCredentials =
    TrustCredentials.CertCollection(tempCertsFile)

  private[this] val address: Address =
    Address(new InetSocketAddress("localhost", 12345))
  private[this] val http2AppProtocols: ApplicationProtocols =
    ApplicationProtocols.Supported(Seq("h2", "http/1.1"))
  private[this] val factory: SslClientEngineFactory =
    ExternalClientEngineFactory

  private[this] def assertEngine(engine: Engine): Unit = {
    val sslEngine = engine.self

    // It's set to be a client.
    assert(sslEngine.getUseClientMode())

    // The host and port are filled in.
    assert(sslEngine.getPeerHost() == "localhost")
    assert(sslEngine.getPeerPort() == 12345)

    // It supports TLSv1.2
    assert(sslEngine.getEnabledProtocols().contains("TLSv1.2"))

    // It supports a standard Cipher Suite
    assert(sslEngine.getEnabledCipherSuites().contains("TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"))
  }

  test("default http/1.1 config can create an engine") {
    val config = SslClientConfiguration()
    val engine = factory(address, config)
    assertEngine(engine)
  }

  test("insecure http/1.1 config can create an engine") {
    val config = SslClientConfiguration(trustCredentials = TrustCredentials.Insecure)
    val engine = factory(address, config)
    assertEngine(engine)
  }

  test("default h2 config can create an engine") {
    val config = SslClientConfiguration(applicationProtocols = http2AppProtocols)
    val engine = factory(address, config)
    assertEngine(engine)
  }

  test("insecure h2 config can create an engine") {
    val config = SslClientConfiguration(
      trustCredentials = TrustCredentials.Insecure,
      applicationProtocols = http2AppProtocols
    )
    val engine = factory(address, config)
    assertEngine(engine)
  }

  test("collection of trust credentials http/1.1 config is not supported") {
    val config = SslClientConfiguration(trustCredentials = certCollection)
    intercept[SslConfigurationException] {
      factory(address, config)
    }
  }

  test("collection of trust credentials h2 config is not supported") {
    val config = SslClientConfiguration(
      trustCredentials = certCollection,
      applicationProtocols = http2AppProtocols
    )
    intercept[SslConfigurationException] {
      factory(address, config)
    }
  }

}
