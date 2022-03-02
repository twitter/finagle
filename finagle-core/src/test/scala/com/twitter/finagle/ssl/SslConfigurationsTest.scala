package com.twitter.finagle.ssl

import javax.net.ssl.SSLContext
import javax.net.ssl.SSLEngine
import org.scalatest.funsuite.AnyFunSuite

class SslConfigurationsTest extends AnyFunSuite {

  private[this] def createTestEngine(): SSLEngine = {
    val sslContext = SSLContext.getInstance("TLSv1.2")
    sslContext.init(null, null, null)
    sslContext.createSSLEngine()
  }

  test("initializeSslContext succeeds for defaults") {
    val protocol =
      if (SslConfigurations.TLSV13Supported) "TLSv1.3"
      else "TLSv1.2"

    val sslContext = SslConfigurations.initializeSslContext(
      KeyCredentials.Unspecified,
      TrustCredentials.Unspecified
    )
    assert(sslContext != null)
    assert(sslContext.getProtocol == protocol)
  }

  test("configureCipherSuites succeeds with good suites") {
    val sslEngine = createTestEngine()
    val cipherSuites = CipherSuites.Enabled(Seq("TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"))
    SslConfigurations.configureCipherSuites(sslEngine, cipherSuites)

    val enabled = sslEngine.getEnabledCipherSuites()
    assert(enabled.length == 1)
    assert(enabled(0) == "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256")
  }

  test("configureCipherSuites fails with bad suites") {
    val sslEngine = createTestEngine()
    val cipherSuites = CipherSuites.Enabled(Seq("TLS_ECDHE_ECDSA_WITH_AES_102_CBC_SHA496"))

    intercept[IllegalArgumentException] {
      SslConfigurations.configureCipherSuites(sslEngine, cipherSuites)
    }
  }

  test("configureProtocols succeeds with good protocols") {
    val sslEngine = createTestEngine()
    val protocols = Protocols.Enabled(Seq("TLSv1.2"))
    SslConfigurations.configureProtocols(sslEngine, protocols)

    val enabled = sslEngine.getEnabledProtocols()
    assert(enabled.length == 1)
    assert(enabled(0) == "TLSv1.2")
  }

  test("configureProtocols fails with bad protocols") {
    val sslEngine = createTestEngine()
    val protocols = Protocols.Enabled(Seq("TLSv2.0"))

    intercept[IllegalArgumentException] {
      SslConfigurations.configureProtocols(sslEngine, protocols)
    }
  }

  test("configureHostnameVerifier turns on endpoint identification when hostname is set") {
    val sslEngine = createTestEngine()
    val sslParametersBefore = sslEngine.getSSLParameters
    assert(sslParametersBefore.getEndpointIdentificationAlgorithm == null)

    SslConfigurations.configureHostnameVerification(sslEngine, Some("twitter.com"))
    val sslParametersAfter = sslEngine.getSSLParameters
    assert(sslParametersAfter.getEndpointIdentificationAlgorithm == "HTTPS")
  }

  test("configureHostnameVerifier leaves endpoint identification off when hostname is empty") {
    val sslEngine = createTestEngine()
    val sslParametersBefore = sslEngine.getSSLParameters
    assert(sslParametersBefore.getEndpointIdentificationAlgorithm == null)

    SslConfigurations.configureHostnameVerification(sslEngine, None)
    val sslParametersAfter = sslEngine.getSSLParameters
    assert(sslParametersAfter.getEndpointIdentificationAlgorithm == null)
  }

  test("checkKeyCredentialsNotSupported does nothing for Unspecified") {
    val keyCredentials = KeyCredentials.Unspecified
    SslConfigurations.checkKeyCredentialsNotSupported("TestFactory", keyCredentials)
  }

  test("checkKeyCredentialsNotSupported throws for CertAndKey") {
    val keyCredentials = KeyCredentials.CertAndKey(null, null)
    val ex = intercept[SslConfigurationException] {
      SslConfigurations.checkKeyCredentialsNotSupported("TestFactory", keyCredentials)
    }
    assert(
      "KeyCredentials.CertAndKey is not supported at this time for TestFactory" ==
        ex.cause.getMessage
    )
  }

  test("checkKeyCredentialsNotSupported throws for CertKeyAndChain") {
    val keyCredentials = KeyCredentials.CertKeyAndChain(null, null, null)
    val ex = intercept[SslConfigurationException] {
      SslConfigurations.checkKeyCredentialsNotSupported("TestFactory", keyCredentials)
    }
    assert(
      "KeyCredentials.CertKeyAndChain is not supported at this time for TestFactory" ==
        ex.cause.getMessage
    )
  }

  test("checkTrustCredentialsNotSupported does nothing for Unspecified") {
    val trustCredentials = TrustCredentials.Unspecified
    SslConfigurations.checkTrustCredentialsNotSupported("TestFactory", trustCredentials)
  }

  test("checkTrustCredentialsNotSupported throws for Insecure") {
    val trustCredentials = TrustCredentials.Insecure
    val ex = intercept[SslConfigurationException] {
      SslConfigurations.checkTrustCredentialsNotSupported("TestFactory", trustCredentials)
    }
    assert(
      "TrustCredentials.Insecure is not supported at this time for TestFactory" ==
        ex.cause.getMessage
    )
  }

  test("checkTrustCredentialsNotSupported throws for TrustManagerFactory") {
    val trustCredentials = TrustCredentials.TrustManagerFactory(null)
    val ex = intercept[SslConfigurationException] {
      SslConfigurations.checkTrustCredentialsNotSupported("TestFactory", trustCredentials)
    }
    assert(
      "TrustCredentials.TrustManagerFactory is not supported at this time for TestFactory" ==
        ex.cause.getMessage
    )
  }

  test("checkTrustCredentialsNotSupported throws for CertCollection") {
    val trustCredentials = TrustCredentials.CertCollection(null)
    val ex = intercept[SslConfigurationException] {
      SslConfigurations.checkTrustCredentialsNotSupported("TestFactory", trustCredentials)
    }
    assert(
      "TrustCredentials.CertCollection is not supported at this time for TestFactory" ==
        ex.cause.getMessage
    )
  }

  test("checkApplicationProtocolsNotSupported does nothing for Unspecified") {
    val appProtocols = ApplicationProtocols.Unspecified
    SslConfigurations.checkApplicationProtocolsNotSupported("TestFactory", appProtocols)
  }

  test("checkApplicationProtocolsNotSupported throws for Supported") {
    val appProtocols = ApplicationProtocols.Supported(Seq("h2"))
    val ex = intercept[SslConfigurationException] {
      SslConfigurations.checkApplicationProtocolsNotSupported("TestFactory", appProtocols)
    }
    assert(
      "ApplicationProtocols.Supported is not supported at this time for TestFactory" ==
        ex.cause.getMessage
    )
  }

  test("checkProtocolsNotSupported throws for Enabled") {
    val protocols = Protocols.Enabled(Seq("TLSv1.2"))
    val ex = intercept[SslConfigurationException] {
      SslConfigurations.checkProtocolsNotSupported("TestFactory", protocols)
    }
    assert(
      "Protocols.Enabled is not supported at this time for TestFactory" ==
        ex.cause.getMessage
    )
  }

  test("checkClientAuthNotSupported throws for Off") {
    val clientAuth = ClientAuth.Off
    val ex = intercept[SslConfigurationException] {
      SslConfigurations.checkClientAuthNotSupported("TestFactory", clientAuth)
    }
    assert(
      "ClientAuth.Off is not supported at this time for TestFactory" ==
        ex.cause.getMessage
    )
  }

  test("checkClientAuthNotSupported throws for Wanted") {
    val clientAuth = ClientAuth.Wanted
    val ex = intercept[SslConfigurationException] {
      SslConfigurations.checkClientAuthNotSupported("TestFactory", clientAuth)
    }
    assert(
      "ClientAuth.Wanted is not supported at this time for TestFactory" ==
        ex.cause.getMessage
    )
  }

  test("checkClientAuthNotSupported throws for Needed") {
    val clientAuth = ClientAuth.Needed
    val ex = intercept[SslConfigurationException] {
      SslConfigurations.checkClientAuthNotSupported("TestFactory", clientAuth)
    }
    assert(
      "ClientAuth.Needed is not supported at this time for TestFactory" ==
        ex.cause.getMessage
    )
  }

}
