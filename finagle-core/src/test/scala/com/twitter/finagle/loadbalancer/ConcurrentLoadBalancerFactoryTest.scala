package com.twitter.finagle.loadbalancer

import com.twitter.finagle._
import com.twitter.finagle.client.{StackClient, StringClient}
import com.twitter.finagle.param.Stats
import com.twitter.finagle.server.StringServer
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.{Await, Future}
import java.net.{InetAddress, InetSocketAddress}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ConcurrentLoadBalancerFactoryTest extends FunSuite with StringClient with StringServer {
  val echoService = Service.mk[String, String](Future.value(_))

  test("makes service factory stack") {
    val address = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val server = stringServer.serve(address, echoService)

    val sr = new InMemoryStatsReceiver
    val clientStack =
      StackClient.newStack.replace(
        LoadBalancerFactory.role, ConcurrentLoadBalancerFactory.module[String, String])
    val client = stringClient.withStack(clientStack)
      .configured(Stats(sr))
      .newService(Name.bound(server.boundAddress), "client")

    assert(sr.counters(Seq("client", "loadbalancer", "adds")) == 4)
    assert(Await.result(client("hello\n")) == "hello")
  }

  test("creates fixed number of service factories based on params") {
    val addr1 = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val server1 = stringServer.serve(addr1, echoService)

    val addr2 = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val server2 = stringServer.serve(addr2, echoService)

    val sr = new InMemoryStatsReceiver
    val clientStack =
      StackClient.newStack.replace(
        LoadBalancerFactory.role, ConcurrentLoadBalancerFactory.module[String, String])
    val client = stringClient.withStack(clientStack)
      .configured(Stats(sr))
      .configured(ConcurrentLoadBalancerFactory.Param(3))
      .newService(Name.bound(server1.boundAddress, server2.boundAddress), "client")

    assert(sr.counters(Seq("client", "loadbalancer", "adds")) == 6)
  }
}
