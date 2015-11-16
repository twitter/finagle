package com.twitter.finagle.memcached.integration

import _root_.java.io.ByteArrayOutputStream
import _root_.java.lang.{Boolean => JBoolean}
import com.twitter.common.application.ShutdownRegistry.ShutdownRegistryImpl
import com.twitter.common.zookeeper.testing.ZooKeeperTestServer
import com.twitter.common.zookeeper.{ServerSets, ZooKeeperClient, ZooKeeperUtils}
import com.twitter.finagle.Memcached
import com.twitter.finagle.cacheresolver.CachePoolConfig
import com.twitter.finagle.memcached.PartitionedClient
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.zookeeper.ZookeeperServerSetCluster
import com.twitter.io.{Buf, Charsets}
import com.twitter.util.Await
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Outcome}

@RunWith(classOf[JUnitRunner])
class Finagle6APITest extends FunSuite with BeforeAndAfter {
  /**
    * Note: This integration test requires a real Memcached server to run.
    */
  var shutdownRegistry: ShutdownRegistryImpl = null
  var testServers = List[TestMemcachedServer]()

  var zkServerSetCluster: ZookeeperServerSetCluster = null
  var zookeeperClient: ZooKeeperClient = null
  val zkPath = "/cache/test/silly-cache"
  var zookeeperServer: ZooKeeperTestServer = null
  var zookeeperServerPort: Int = 0

  before {
    // start zookeeper server and create zookeeper client
    shutdownRegistry = new ShutdownRegistryImpl
    zookeeperServer = new ZooKeeperTestServer(0, shutdownRegistry)
    zookeeperServer.startNetwork()
    zookeeperServerPort = zookeeperServer.getPort()

    // connect to zookeeper server
    zookeeperClient = zookeeperServer.createClient(ZooKeeperClient.digestCredentials("user","pass"))

    // create serverset
    val serverSet = ServerSets.create(zookeeperClient, ZooKeeperUtils.EVERYONE_READ_CREATOR_ALL, zkPath)
    zkServerSetCluster = new ZookeeperServerSetCluster(serverSet)

    // start five memcached server and join the cluster
    (0 to 4) foreach { _ =>
      TestMemcachedServer.start() match {
        case Some(server) =>
          testServers :+= server
          zkServerSetCluster.join(server.address)
        case None =>
          throw new Exception("could not start TestMemcachedServer")
      }
    }

    if (!testServers.isEmpty) {
      // set cache pool config node data
      val cachePoolConfig: CachePoolConfig = new CachePoolConfig(cachePoolSize = 5)
      val output: ByteArrayOutputStream = new ByteArrayOutputStream
      CachePoolConfig.jsonCodec.serialize(cachePoolConfig, output)
      zookeeperClient.get().setData(zkPath, output.toByteArray, -1)
    }
  }

  after {
    // shutdown zookeeper server and client
    shutdownRegistry.execute()

    if (!testServers.isEmpty) {
      // shutdown memcached server
      testServers foreach { _.stop() }
      testServers = List()
    }
  }

  override def withFixture(test: NoArgTest): Outcome = {
    if (!testServers.isEmpty) test()
    else {
      info("Cannot start memcached. Skipping test...")
      cancel()
    }
  }

  if (!Option(System.getProperty("SKIP_FLAKY")).isDefined) {
    test("with unmanaged regular zk serverset") {
      val client = Memcached.client.newTwemcacheClient(
        "zk!localhost:"+zookeeperServerPort+"!"+zkPath).asInstanceOf[PartitionedClient]

      // Wait for group to contain members
      Thread.sleep(5000)

      val count = 100
        (0 until count).foreach{
          n => {
            client.set("foo"+n, Buf.Utf8("bar"+n))()
          }
        }

      (0 until count).foreach {
        n => {
          val c = client.clientOf("foo"+n)
          val Buf.Utf8(res) = c.get("foo"+n)().get
          assert(res == "bar"+n)
        }
      }
    }
  }

  if (!Option(System.getProperty("SKIP_FLAKY")).isDefined)
    test("with managed cache pool") {
      val client = Memcached.client.newTwemcacheClient(
        "twcache!localhost:"+zookeeperServerPort+"!"+zkPath).asInstanceOf[PartitionedClient]

      // Wait for group to contain members
      Thread.sleep(5000)

      client.delete("foo")()
      assert(client.get("foo")() == None)
      client.set("foo", Buf.Utf8("bar"))()

      val Buf.Utf8(res) = client.get("foo")().get
      assert(res == "bar")

      val count = 100
        (0 until count).foreach{
          n => {
            client.set("foo"+n, Buf.Utf8("bar"+n))()
          }
        }

      (0 until count).foreach {
        n => {
          val c = client.clientOf("foo"+n)
          val Buf.Utf8(res) = c.get("foo"+n)().get
          assert(res == "bar"+n)
        }
      }
    }

  test("with static servers list") {
    val client = Memcached.client.newRichClient(
      "twcache!localhost:%d,localhost:%d".format(testServers(0).address.getPort, testServers(1).address.getPort))

    Await.result(client.delete("foo"))
    assert(Await.result(client.get("foo")) == None)
    Await.result(client.set("foo", Buf.Utf8("bar")))
    val Buf.Utf8(res) = Await.result(client.get("foo")).get
    assert(res == "bar")
  }
}
