package com.twitter.finagle.param

import com.twitter.finagle.Address
import com.twitter.finagle.server.utils.StringServer
import com.twitter.finagle.ssl.Engine
import com.twitter.finagle.ssl.server.{
  SslServerConfiguration,
  SslServerEngineFactory,
  SslServerSessionVerifier
}
import com.twitter.finagle.transport.Transport
import javax.net.ssl.SSLSession
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class ServerTransportParamsTest extends AnyFunSuite with MockitoSugar {

  private val config = SslServerConfiguration()
  private val engine = mock[Engine]
  private val engineFactory = new SslServerEngineFactory {
    def apply(config: SslServerConfiguration): Engine = engine
  }
  private val sessionVerifier = new SslServerSessionVerifier {
    def apply(address: Address, config: SslServerConfiguration, session: SSLSession): Boolean = true
  }

  test("withTransport.tls sets SSL/TLS configuration") {
    val server = StringServer.server.withTransport.tls(config)
    assert(server.params[Transport.ServerSsl].sslServerConfiguration == Some(config))
  }

  test("withTransport.tls sets configuration, engine factory") {
    val server = StringServer.server.withTransport.tls(config, engineFactory)
    assert(server.params[Transport.ServerSsl].sslServerConfiguration == Some(config))
    assert(server.params[SslServerEngineFactory.Param].factory == engineFactory)
  }

  test("withTransport.tls sets configuration, verifier") {
    val server = StringServer.server.withTransport.tls(config, sessionVerifier)
    assert(server.params[Transport.ServerSsl].sslServerConfiguration == Some(config))
    assert(server.params[SslServerSessionVerifier.Param].verifier == sessionVerifier)
  }

  test("withTransport.tls sets configuration, engine factory, verifier") {
    val server = StringServer.server.withTransport.tls(config, engineFactory, sessionVerifier)
    assert(server.params[Transport.ServerSsl].sslServerConfiguration == Some(config))
    assert(server.params[SslServerEngineFactory.Param].factory == engineFactory)
    assert(server.params[SslServerSessionVerifier.Param].verifier == sessionVerifier)
  }

}
