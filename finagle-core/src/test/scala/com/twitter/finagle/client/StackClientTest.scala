package com.twitter.finagle.client

import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.{param, Name}
import java.net.InetSocketAddress
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}

@RunWith(classOf[JUnitRunner])
class StackClientTest extends FunSuite with StringClient with AssertionsForJUnit {
  trait Ctx {
    val sr = new InMemoryStatsReceiver
    val client = stringClient
      .configured(param.Stats(sr))
  }

  test("client stats are scoped to label")(new Ctx {
    // use dest when no label is set
    client.newService("inet!localhost:8080")
    eventually {
      assert(sr.counters(Seq("inet!localhost:8080", "loadbalancer", "adds")) === 1)
    }

    // use param.Label when set
    client.configured(param.Label("myclient")).newService("localhost:8080")
    eventually {
      assert(sr.counters(Seq("myclient", "loadbalancer", "adds")) === 1)
    }

    // use evaled label when both are set
    client.configured(param.Label("myclient")).newService("othername=localhost:8080")
    eventually {
      assert(sr.counters(Seq("othername", "loadbalancer", "adds")) === 1)
    }
  })

  test("Client added to client registry") (new Ctx {
    val name = "clientTest"

    assert(ClientRegistry.clientInfo(name).isEmpty)
    assert(!ClientRegistry.clientList().contains(name))

    client.newClient(Name.bound(new InetSocketAddress(8080)), name)
    
    assert(ClientRegistry.clientList().contains(name))
    assert(!ClientRegistry.clientInfo(name).isEmpty)
  })
}
