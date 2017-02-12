package com.twitter.finagle.serverset2.client.apache

import com.twitter.conversions.time._
import com.twitter.finagle.serverset2.client._
import com.twitter.util.Timer
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import com.twitter.finagle.stats.DefaultStatsReceiver

@RunWith(classOf[JUnitRunner])
class ClientBuilderTest extends FlatSpec {
  val config = ClientConfig(
    hosts = "localhost:2181",
    sessionTimeout = 10.seconds,
    statsReceiver = DefaultStatsReceiver,
    readOnlyOK = false,
    sessionId = None,
    password = None,
    timer = Timer.Nil
  )

  "reader" should "return Apache ZK reader" in {
    val zkr = ClientBuilder().reader()

    assert(zkr.value.isInstanceOf[ZooKeeperReader])
    assert(zkr.value.getClass == ApacheZooKeeper.newClient(config).value.getClass)
  }

  "writer" should "return Apache ZK writer" in {
    val zkw = ClientBuilder().writer()

    assert(zkw.value.isInstanceOf[ZooKeeperRW])
    assert(zkw.value.getClass == ApacheZooKeeper.newClient(config).value.getClass)
  }

  "multi" should "raise RuntimeException" in {
    intercept[RuntimeException] {
      ClientBuilder().multi()
    }
  }
}
