package com.twitter.finagle.param

import com.twitter.finagle.Address
import com.twitter.finagle.client.utils.StringClient
import com.twitter.finagle.ssl.Engine
import com.twitter.finagle.ssl.client.{
  SslClientConfiguration,
  SslClientEngineFactory,
  SslClientSessionVerifier
}
import com.twitter.finagle.transport.Transport
import javax.net.ssl.SSLSession
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class ClientTransportParamsTest extends AnyFunSuite with MockitoSugar {

  private val config = SslClientConfiguration()
  private val engine = mock[Engine]
  private val engineFactory = new SslClientEngineFactory {
    def apply(address: Address, config: SslClientConfiguration): Engine = engine
  }
  private val sessionVerifier = new SslClientSessionVerifier {
    def apply(address: Address, config: SslClientConfiguration, session: SSLSession): Boolean = true
  }

  test("withTransport.tls sets SSL/TLS configuration") {
    val client = StringClient.client.withTransport.tls(config)
    assert(client.params[Transport.ClientSsl].sslClientConfiguration == Some(config))
  }

  test("withTransport.tls sets SSL/TLS configuration, engine factory") {
    val client = StringClient.client.withTransport.tls(config, engineFactory)
    assert(client.params[Transport.ClientSsl].sslClientConfiguration == Some(config))
    assert(client.params[SslClientEngineFactory.Param].factory == engineFactory)
  }

  test("withTransport.tls sets SSL/TLS configuration, verifier") {
    val client = StringClient.client.withTransport.tls(config, sessionVerifier)
    assert(client.params[Transport.ClientSsl].sslClientConfiguration == Some(config))
    assert(client.params[SslClientSessionVerifier.Param].verifier == sessionVerifier)
  }

  test("withTransport.tls sets SSL/TLS configuration, engine factory, verifier") {
    val client = StringClient.client.withTransport.tls(config, engineFactory, sessionVerifier)
    assert(client.params[Transport.ClientSsl].sslClientConfiguration == Some(config))
    assert(client.params[SslClientEngineFactory.Param].factory == engineFactory)
    assert(client.params[SslClientSessionVerifier.Param].verifier == sessionVerifier)
  }

}
