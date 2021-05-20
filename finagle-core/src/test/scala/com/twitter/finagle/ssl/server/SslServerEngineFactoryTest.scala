package com.twitter.finagle.ssl.server

import com.twitter.finagle.ssl.{CipherSuites, ClientAuth, Engine, Protocols}
import javax.net.ssl.{SSLContext, SSLEngine}
import org.scalatest.funsuite.AnyFunSuite

class SslServerEngineFactoryTest extends AnyFunSuite {

  private[this] def createTestSslContext(): SSLContext = {
    val sslContext = SSLContext.getInstance("TLSv1.2")
    sslContext.init(null, null, null)
    sslContext
  }

  private[this] def createTestSslEngine(): SSLEngine = {
    val sslContext = createTestSslContext()
    sslContext.createSSLEngine()
  }

  private[this] def createTestEngine(): Engine =
    new Engine(createTestSslEngine())

  test("configureClientAuth Unspecified doesn't change anything") {
    val sslEngine = createTestSslEngine()
    sslEngine.setWantClientAuth(true)

    SslServerEngineFactory.configureClientAuth(sslEngine, ClientAuth.Unspecified)

    assert(sslEngine.getWantClientAuth())
    assert(!sslEngine.getNeedClientAuth())
  }

  test("configureClientAuth Off turns off client authentication") {
    val sslEngine = createTestSslEngine()
    sslEngine.setWantClientAuth(true)

    SslServerEngineFactory.configureClientAuth(sslEngine, ClientAuth.Off)

    assert(!sslEngine.getWantClientAuth())
    assert(!sslEngine.getNeedClientAuth())
  }

  test("configureClientAuth Wanted turns on desired client authentication") {
    val sslEngine = createTestSslEngine()
    SslServerEngineFactory.configureClientAuth(sslEngine, ClientAuth.Wanted)
    assert(sslEngine.getWantClientAuth())
    assert(!sslEngine.getNeedClientAuth())
  }

  test("configureClientAuth Needed turns on required client authentication") {
    val sslEngine = createTestSslEngine()
    SslServerEngineFactory.configureClientAuth(sslEngine, ClientAuth.Needed)
    assert(!sslEngine.getWantClientAuth())
    assert(sslEngine.getNeedClientAuth())
  }

  test("configureEngine sets server mode, protocols, and cipher suites") {
    val engine = createTestEngine()
    val protocols = Protocols.Enabled(Seq("TLSv1.2"))
    val cipherSuites = CipherSuites.Enabled(Seq("TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384"))
    val config = SslServerConfiguration(protocols = protocols, cipherSuites = cipherSuites)

    SslServerEngineFactory.configureEngine(engine, config)
    val sslEngine = engine.self

    // is a server engine
    assert(!sslEngine.getUseClientMode())

    // has the right protocols
    val enabledProtocols = sslEngine.getEnabledProtocols()
    assert(enabledProtocols.length == 1)
    assert(enabledProtocols(0) == "TLSv1.2")

    // has the right cipher suites
    val enabledCipherSuites = sslEngine.getEnabledCipherSuites()
    assert(enabledCipherSuites.length == 1)
    assert(enabledCipherSuites(0) == "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384")
  }

  test("createEngine can create an engine from an SSLContext") {
    val sslContext = createTestSslContext()
    val engine = SslServerEngineFactory.createEngine(sslContext)
    assert(engine != null)
    assert(engine.self != null)
  }

}
