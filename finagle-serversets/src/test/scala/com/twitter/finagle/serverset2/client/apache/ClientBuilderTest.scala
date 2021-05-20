package com.twitter.finagle.serverset2.client.apache

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.serverset2.client._
import com.twitter.finagle.stats.DefaultStatsReceiver
import com.twitter.util.Timer
import org.scalatest.funsuite.AnyFunSuite

class ClientBuilderTest extends AnyFunSuite {
  val config = ClientConfig(
    hosts = "localhost:2181",
    sessionTimeout = 10.seconds,
    statsReceiver = DefaultStatsReceiver,
    readOnlyOK = false,
    sessionId = None,
    password = None,
    timer = Timer.Nil
  )

  test("ClientBuilder.reader returns an Apache ZK reader") {
    val zkr = ClientBuilder().reader()

    assert(zkr.value.isInstanceOf[ZooKeeperReader])
    assert(zkr.value.getClass == ApacheZooKeeper.newClient(config).value.getClass)
  }

  test("ClientBuilder.writer returns an Apache ZK writer") {
    val zkw = ClientBuilder().writer()

    assert(zkw.value.isInstanceOf[ZooKeeperRW])
    assert(zkw.value.getClass == ApacheZooKeeper.newClient(config).value.getClass)
  }

  test("ClientBuilder.multi raises a RuntimeException") {
    intercept[RuntimeException] {
      ClientBuilder().multi()
    }
  }
}
