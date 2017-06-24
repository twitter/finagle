package com.twitter.finagle.builder

import com.twitter.finagle._
import com.twitter.finagle.param.ProtocolLibrary
import com.twitter.finagle.server.StringServer
import com.twitter.util._
import com.twitter.util.registry.{GlobalRegistry, SimpleRegistry}
import java.net.{InetAddress, InetSocketAddress}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.concurrent.{Eventually, IntegrationPatience}

@RunWith(classOf[JUnitRunner])
class ServerBuilderTest extends FunSuite
  with Eventually
  with IntegrationPatience
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
}
