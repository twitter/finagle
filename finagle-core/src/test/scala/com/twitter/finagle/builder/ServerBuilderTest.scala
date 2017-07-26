package com.twitter.finagle.builder

import com.twitter.finagle._
import com.twitter.finagle.param.ProtocolLibrary
import com.twitter.finagle.server.StringServer
import com.twitter.finagle.ssl.Engine
import com.twitter.finagle.ssl.server.{
  SslServerConfiguration,
  SslServerEngineFactory,
  SslServerSessionVerifier
}
import com.twitter.finagle.transport.Transport
import com.twitter.util._
import com.twitter.util.registry.{GlobalRegistry, SimpleRegistry}
import java.net.{InetAddress, InetSocketAddress}
import javax.net.ssl.SSLSession
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.mockito.MockitoSugar

@RunWith(classOf[JUnitRunner])
class ServerBuilderTest
    extends FunSuite
    with Eventually
    with IntegrationPatience
    with MockitoSugar
    with StringServer {

  test(s"registers server with bound address") {
    val simple = new SimpleRegistry()

    GlobalRegistry.withRegistry(simple) {
      val server = ServerBuilder()
        .stack(stringServer)
        .bindTo(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
        .name("test")
        .build(Service.const(Future.value("hi")))

      val entries = simple.toSet
      val specified = entries.filter(_.key.startsWith(Seq("server", "string", "test")))
      // Entries are in the form: Entry(List(server, fancy, test, /127.0.0.1:58904, RequestStats, unit),MILLISECONDS)
      val entry = specified.head // data is repeated as entry.key, just take the first
      val hostAndPort = entry.key.filter(_.contains("127.0.0.1")).head
      assert(!hostAndPort.contains(":0"), "unbounded address in server registry")
      server.close()
    }
  }

  test("#configured[P](P)(Stack.Param[P]) should pass name through") {
    val sb = ServerBuilder()
    assert(!sb.params.contains[ProtocolLibrary])
    val configured = sb.configured(ProtocolLibrary("foo"))
    assert(configured.params.contains[ProtocolLibrary])
    assert("foo" == configured.params[ProtocolLibrary].name)
  }

  private val config = SslServerConfiguration()
  private val engine = mock[Engine]
  private val engineFactory = new SslServerEngineFactory {
    def apply(config: SslServerConfiguration): Engine = engine
  }
  private val sessionVerifier = new SslServerSessionVerifier {
    def apply(config: SslServerConfiguration, session: SSLSession): Boolean = true
  }

  test("ServerBuilder sets SSL/TLS configuration") {
    val server = ServerBuilder().tls(config)
    assert(server.params[Transport.ServerSsl].sslServerConfiguration == Some(config))
  }

  test("ServerBuilder sets SSL/TLS configuration, engine factory") {
    val server = ServerBuilder().tls(config, engineFactory)
    assert(server.params[Transport.ServerSsl].sslServerConfiguration == Some(config))
    assert(server.params[SslServerEngineFactory.Param].factory == engineFactory)
  }

  test("ServerBuilder sets SSL/TLS configuration, verifier") {
    val server = ServerBuilder().tls(config, sessionVerifier)
    assert(server.params[Transport.ServerSsl].sslServerConfiguration == Some(config))
    assert(server.params[SslServerSessionVerifier.Param].verifier == sessionVerifier)
  }

  test("ServerBuilder sets SSL/TLS configuration, engine factory, verifier") {
    val server = ServerBuilder().tls(config, engineFactory, sessionVerifier)
    assert(server.params[Transport.ServerSsl].sslServerConfiguration == Some(config))
    assert(server.params[SslServerEngineFactory.Param].factory == engineFactory)
    assert(server.params[SslServerSessionVerifier.Param].verifier == sessionVerifier)
  }

}
