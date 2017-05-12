package com.twitter.finagle.ssl.client

import com.twitter.finagle.Address
import com.twitter.finagle.ssl.{CipherSuites, Engine, Protocols}
import java.net.InetSocketAddress
import javax.net.ssl.SSLContext
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SslClientEngineFactoryTest extends FunSuite {

  private[this] val isa = new InetSocketAddress("localhost", 12345)

  private[this] def createTestSslContext(): SSLContext = {
    val sslContext = SSLContext.getInstance("TLSv1.2")
    sslContext.init(null, null, null)
    sslContext
  }

  private[this] def createTestEngine(): Engine = {
    val sslContext = createTestSslContext()
    new Engine(sslContext.createSSLEngine())
  }

  test("getHostname with config.hostname set") {
    val config = SslClientConfiguration(hostname = Some("localhost.twitter.com"))
    assert("localhost.twitter.com" == SslClientEngineFactory.getHostname(isa, config))
  }

  test("getHostname without config.hostname set") {
    val config = SslClientConfiguration()
    assert("localhost" == SslClientEngineFactory.getHostname(isa, config))
  }

  test("configureEngine sets client mode, protocols, and cipher suites") {
    val engine = createTestEngine()
    val protocols = Protocols.Enabled(Seq("TLSv1.2"))
    val cipherSuites = CipherSuites.Enabled(Seq("TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384"))
    val config = SslClientConfiguration(protocols = protocols, cipherSuites = cipherSuites)

    SslClientEngineFactory.configureEngine(engine, config)
    val sslEngine = engine.self

    // is a client engine
    assert(sslEngine.getUseClientMode())

    // has the right protocols
    val enabledProtocols = sslEngine.getEnabledProtocols()
    assert(enabledProtocols.length == 1)
    assert(enabledProtocols(0) == "TLSv1.2")

    // has the right cipher suites
    val enabledCipherSuites = sslEngine.getEnabledCipherSuites()
    assert(enabledCipherSuites.length == 1)
    assert(enabledCipherSuites(0) == "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384")
  }

  test("createEngine uses inet hostname when not configured") {
    val context = createTestSslContext()
    val address = Address(isa)
    val config = SslClientConfiguration()
    val engine = SslClientEngineFactory.createEngine(context, address, config)
    val sslEngine = engine.self

    assert(sslEngine.getPeerHost() == "localhost")
    assert(sslEngine.getPeerPort() == 12345)
  }

  test("createEngine uses config hostname when configured") {
    val context = createTestSslContext()
    val address = Address(isa)
    val config = SslClientConfiguration(hostname = Some("testname"))
    val engine = SslClientEngineFactory.createEngine(context, address, config)
    val sslEngine = engine.self

    assert(sslEngine.getPeerHost() == "testname")
    assert(sslEngine.getPeerPort() == 12345)
  }

  test("createEngine uses no hostname/port when not an Address.Inet") {
    val context = createTestSslContext()
    val address = Address.Failed(new Exception("Testing"))
    val config = SslClientConfiguration()
    val engine = SslClientEngineFactory.createEngine(context, address, config)
    val sslEngine = engine.self

    assert(sslEngine.getPeerHost() == null)
    assert(sslEngine.getPeerPort() == -1)
  }

}
