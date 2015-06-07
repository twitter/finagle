package com.twitter.finagle.loadbalancer

import com.twitter.finagle._
import com.twitter.finagle.client.StringClient
import com.twitter.finagle.param.Stats
import com.twitter.finagle.server.StringServer
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.{Await, Future}
import java.net.{InetAddress, InetSocketAddress}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LoadBalancerFactoryTest extends FunSuite with StringClient with StringServer {
  val echoService = Service.mk[String, String](Future.value(_))

  test("make service factory stack") {
    val addr1 = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val server1 = stringServer.serve(addr1, echoService)

    val addr2 = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val server2 = stringServer.serve(addr2, echoService)

    val sr = new InMemoryStatsReceiver
    val client = stringClient
        .configured(Stats(sr))
        .newService(Name.bound(server1.boundAddress, server2.boundAddress), "client")

    assert(sr.counters(Seq("client", "loadbalancer", "adds")) == 2)
    assert(Await.result(client("hello\n")) == "hello")
  }
}