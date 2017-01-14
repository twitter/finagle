package com.twitter.finagle.ssl.client

import com.twitter.finagle.ssl.{
  ApplicationProtocols, KeyCredentials, SslConfigurationException, TrustCredentials}
import java.net.InetSocketAddress
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SslClientEngineFactoryTest extends FunSuite {

  private[this] val isa = new InetSocketAddress("localhost", 12345)

  test("getHostname with config.hostname set") {
    val config = SslClientConfiguration(hostname = Some("localhost.twitter.com"))
    assert("localhost.twitter.com" == SslClientEngineFactory.getHostname(isa, config))
  }

  test("getHostname without config.hostname set") {
    val config = SslClientConfiguration()
    assert("localhost" == SslClientEngineFactory.getHostname(isa, config))
  }

  test("keyCredentialsNotSupported does nothing for Unspecified") {
    val config = SslClientConfiguration()
    SslClientEngineFactory.checkKeyCredentialsNotSupported("TestFactory", config)
  }

  test("keyCredentialsNotSupported throws for CertAndKey") {
    val config = SslClientConfiguration(keyCredentials = KeyCredentials.CertAndKey(null, null))
    val ex = intercept[SslConfigurationException] {
      SslClientEngineFactory.checkKeyCredentialsNotSupported("TestFactory", config)
    }
    assert("KeyCredentials.CertAndKey is not supported at this time for TestFactory" ==
      ex.getMessage)
  }

  test("keyCredentialsNotSupported throws for CertKeyAndChain") {
    val config = SslClientConfiguration(keyCredentials = KeyCredentials.CertKeyAndChain(null, null, null))
    val ex = intercept[SslConfigurationException] {
      SslClientEngineFactory.checkKeyCredentialsNotSupported("TestFactory", config)
    }
    assert("KeyCredentials.CertKeyAndChain is not supported at this time for TestFactory" ==
      ex.getMessage)
  }

  test("trustCredentialsNotSupported does nothing for Unspecified") {
    val config = SslClientConfiguration()
    SslClientEngineFactory.checkTrustCredentialsNotSupported("TestFactory", config)
  }

  test("trustCredentialsNotSupported throws for Insecure") {
    val config = SslClientConfiguration(trustCredentials = TrustCredentials.Insecure)
    val ex = intercept[SslConfigurationException] {
      SslClientEngineFactory.checkTrustCredentialsNotSupported("TestFactory", config)
    }
    assert("TrustCredentials.Insecure is not supported at this time for TestFactory" ==
      ex.getMessage)
  }

  test("trustCredentialsNotSupported throws for CertCollection") {
    val config = SslClientConfiguration(trustCredentials = TrustCredentials.CertCollection(null))
    val ex = intercept[SslConfigurationException] {
      SslClientEngineFactory.checkTrustCredentialsNotSupported("TestFactory", config)
    }
    assert("TrustCredentials.CertCollection is not supported at this time for TestFactory" ==
      ex.getMessage)
  }

  test("applicationProtocolsNotSupported does nothing for Unspecified") {
    val config = SslClientConfiguration()
    SslClientEngineFactory.checkApplicationProtocolsNotSupported("TestFactory", config)
  }

  test("applicationProtocolsNotSupported throws for Supported") {
    val appProtocols = ApplicationProtocols.Supported(Seq("h2"))
    val config = SslClientConfiguration(applicationProtocols = appProtocols)
    val ex = intercept[SslConfigurationException] {
      SslClientEngineFactory.checkApplicationProtocolsNotSupported("TestFactory", config)
    }
    assert("ApplicationProtocols.Supported is not supported at this time for TestFactory" ==
      ex.getMessage)
  }

}
