package com.twitter.finagle.memcached.integration

import com.twitter.common.io.FileUtils._
import com.twitter.common.quantity.{Amount, Time}
import com.twitter.common.zookeeper.{ServerSets, ZooKeeperClient, ZooKeeperUtils}
import com.twitter.conversions.time._
import com.twitter.finagle.Memcached
import com.twitter.finagle.cacheresolver.CachePoolConfig
import com.twitter.finagle.memcached.migration._
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.zookeeper.ZookeeperServerSetCluster
import com.twitter.io.Buf
import com.twitter.util._
import com.twitter.zk.ServerCnxnFactory
import java.io.ByteArrayOutputStream
import java.net.{InetAddress, InetSocketAddress}
import org.apache.zookeeper.server.ZooKeeperServer
import org.junit.runner.RunWith
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, BeforeAndAfterEach, FunSuite}

@RunWith(classOf[JUnitRunner])
class MigrationClientTest extends FunSuite with BeforeAndAfterEach with BeforeAndAfter with Eventually with IntegrationPatience {
  /**
   * Note: This integration test requires a real Memcached server to run.
   */
  val basePath = "/cache/test/silly-cache"
  val oldPoolPath = basePath + "/OldPool"
  val newPoolPath = basePath + "/NewPool"

  var zookeeperServer: ZooKeeperServer = null
  var zookeeperServerPort: Int = 0
  var zookeeperClient: ZooKeeperClient = null
  var connectionFactory: ServerCnxnFactory = null

  var testServers: List[TestMemcachedServer] = List()

  val TIMEOUT = 15.seconds

  override def beforeEach() {
    val loopback = InetAddress.getLoopbackAddress

    // start zookeeper server and create zookeeper client
    zookeeperServer = new ZooKeeperServer(
      createTempDir(),
      createTempDir(),
      ZooKeeperServer.DEFAULT_TICK_TIME)
    connectionFactory = ServerCnxnFactory(loopback)
    connectionFactory.startup(zookeeperServer)
    zookeeperServerPort = zookeeperServer.getClientPort

    zookeeperClient = new ZooKeeperClient(
      Amount.of(10, Time.MILLISECONDS),
      new InetSocketAddress(loopback, zookeeperServerPort))

    // set-up old pool
    val oldPoolCluster = new ZookeeperServerSetCluster(
      ServerSets.create(zookeeperClient, ZooKeeperUtils.OPEN_ACL_UNSAFE, oldPoolPath))
    (0 to 1) foreach { _ =>
      TestMemcachedServer.start() match {
        case Some(server) =>
          oldPoolCluster.join(server.address)
          testServers :+= server
        case None => fail("Cannot start memcached.")
      }
    }

    // set-up new pool
    val newPoolCluster = new ZookeeperServerSetCluster(
      ServerSets.create(zookeeperClient, ZooKeeperUtils.OPEN_ACL_UNSAFE, newPoolPath))
    (0 to 1) foreach { _ =>
      TestMemcachedServer.start() match {
        case Some(server) =>
          newPoolCluster.join(server.address)
          testServers :+= server
        case None => fail("Cannot start memcached.")
      }
    }

    // set config data
    val cachePoolConfig: CachePoolConfig = new CachePoolConfig(cachePoolSize = 2)
    val output: ByteArrayOutputStream = new ByteArrayOutputStream
    CachePoolConfig.jsonCodec.serialize(cachePoolConfig, output)
    zookeeperClient.get().setData(oldPoolPath, output.toByteArray, -1)
    zookeeperClient.get().setData(newPoolPath, output.toByteArray, -1)

    val migrationConfig = MigrationConstants.MigrationConfig("Pending", false, false)
    val migrationDataArray = MigrationConstants.jsonMapper.writeValueAsString(migrationConfig).getBytes()
    zookeeperClient.get().setData(basePath, migrationDataArray, -1)
  }

  override def afterEach()  {
    zookeeperClient.close()

    connectionFactory.shutdown()

    // shutdown memcached server
    testServers foreach { _.stop() }
    testServers = List()
  }

  if (!sys.props.contains("SKIP_FLAKY")) // CSL-1719
  test("not migrating yet") {
    val client1 = Memcached.client.newRichClient(
      dest = "twcache!localhost:"+zookeeperServerPort+"!"+oldPoolPath)
    val client2 = Memcached.client.newRichClient(
      dest = "twcache!localhost:"+zookeeperServerPort+"!"+newPoolPath)
    val migrationClient = MigrationClient.newMigrationClient("localhost:"+zookeeperServerPort, basePath)
    migrationClient.loadZKData() // force loading the config to fully set-up the client

    eventually { Await.result(migrationClient.get("test")) }

    assert(Await.result(migrationClient.get("foo"), TIMEOUT) == None)
    Await.result(migrationClient.set("foo", Buf.Utf8("bar")), TIMEOUT)
    val Buf.Utf8(res) = Await.result(migrationClient.get("foo"), TIMEOUT).get
    assert(res == "bar")

    val Buf.Utf8(client1Res) = Await.result(client1.get("foo"), TIMEOUT).get
    assert(client1Res == "bar")
    eventually { assert(Await.result(client2.get("foo")) == None) }
  }

  if (!sys.props.contains("SKIP_FLAKY")) {
    test("sending dark traffic") {
      val migrationConfig = MigrationConstants.MigrationConfig("Warming", false, false)
      val migrationDataArray = MigrationConstants.jsonMapper.writeValueAsString(migrationConfig)
      zookeeperClient.get().setData(basePath, migrationDataArray, -1)

      val client1 = Memcached.client.newRichClient(
        dest = "twcache!localhost:"+zookeeperServerPort+"!"+oldPoolPath)
      val client2 = Memcached.client.newRichClient(
        dest = "twcache!localhost:"+zookeeperServerPort+"!"+newPoolPath)
      val migrationClient = MigrationClient.newMigrationClient("localhost:"+zookeeperServerPort, basePath)
      migrationClient.loadZKData() // force loading the config to fully set-up the client

      eventually { Await.result(migrationClient.get("test")) }

      assert(Await.result(migrationClient.get("foo"), TIMEOUT) == None)
      Await.result(migrationClient.set("foo", Buf.Utf8("bar")), TIMEOUT)

      assert(Await.result(migrationClient.get("foo"), TIMEOUT).get == Buf.Utf8("bar"))

      assert(Await.result(client1.get("foo"), TIMEOUT).get == Buf.Utf8("bar"))
      eventually { assert(Await.result(client2.get("foo")).map { case Buf.Utf8(s) => s } == Some("bar")) }
    }
  }

  if (!sys.props.contains("SKIP_FLAKY")) // CSL-1731
  test("dark read w/ read repair") {
    val migrationConfig = MigrationConstants.MigrationConfig("Warming", true, false)
    val migrationDataArray = MigrationConstants.jsonMapper.writeValueAsString(migrationConfig)
    zookeeperClient.get().setData(basePath, migrationDataArray, -1)

    val client1 = Memcached.client.newRichClient(
      dest = "twcache!localhost:"+zookeeperServerPort+"!"+oldPoolPath)
    val client2 = Memcached.client.newRichClient(
      dest = "twcache!localhost:"+zookeeperServerPort+"!"+newPoolPath)
    val migrationClient = MigrationClient.newMigrationClient("localhost:"+zookeeperServerPort, basePath)
    migrationClient.loadZKData() // force loading the config to fully set-up the client

    eventually { Await.result(migrationClient.get("test")) }

    Await.result(client1.set("foo", Buf.Utf8("bar")), TIMEOUT)
    val Buf.Utf8(res) = Await.result(client1.get("foo"), TIMEOUT).get
    assert(res == "bar")
    assert(Await.result(client2.get("foo"), TIMEOUT) == None)

    val Buf.Utf8(mcRes) = Await.result(migrationClient.get("foo"), TIMEOUT).get
    assert(mcRes == "bar")

    val Buf.Utf8(cl1Res) = Await.result(client1.get("foo"), TIMEOUT).get
    assert(cl1Res == "bar")
    eventually { assert(Await.result(client2.get("foo")).map { case Buf.Utf8(s) => s } == Some("bar")) }
  }

  if (!sys.props.contains("SKIP_FLAKY")) // CSL-1731
  test("use new pool with fallback to old pool") {
    val migrationConfig = MigrationConstants.MigrationConfig("Verifying", false, false)
    val migrationDataArray = MigrationConstants.jsonMapper.writeValueAsString(migrationConfig)
    zookeeperClient.get().setData(basePath, migrationDataArray, -1)

    val client1 = Memcached.client.newRichClient(
      dest = "twcache!localhost:"+zookeeperServerPort+"!"+oldPoolPath)
    val client2 = Memcached.client.newRichClient(
      dest = "twcache!localhost:"+zookeeperServerPort+"!"+newPoolPath)
    val migrationClient = MigrationClient.newMigrationClient("localhost:"+zookeeperServerPort, basePath)
    migrationClient.loadZKData() // force loading the config to fully set-up the client

    eventually { Await.result(migrationClient.get("test")) }

    Await.result(client1.set("foo", Buf.Utf8("bar")), TIMEOUT)
    val Buf.Utf8(res) = Await.result(client1.get("foo"), TIMEOUT).get
    assert(res == "bar")
    assert(Await.result(client2.get("foo"), TIMEOUT) == None)

    val Buf.Utf8(res2) = Await.result(migrationClient.get("foo"), TIMEOUT).get
    assert(res2 == "bar")

    val Buf.Utf8(res3) = Await.result(client1.get("foo"), TIMEOUT).get
    assert(res3 == "bar")
    eventually { assert(Await.result(client2.get("foo")) == None) }
  }

  if (!sys.props.contains("SKIP_FLAKY")) // CSL-1731
  test("use new pool with fallback to old pool and readrepair") {
    val migrationConfig = MigrationConstants.MigrationConfig("Verifying", false, true)
    val migrationDataArray = MigrationConstants.jsonMapper.writeValueAsString(migrationConfig)
    zookeeperClient.get().setData(basePath, migrationDataArray, -1)

    val client1 = Memcached.client.newRichClient(
      dest = "twcache!localhost:"+zookeeperServerPort+"!"+oldPoolPath)
    val client2 = Memcached.client.newRichClient(
      dest = "twcache!localhost:"+zookeeperServerPort+"!"+newPoolPath)
    val migrationClient = MigrationClient.newMigrationClient("localhost:"+zookeeperServerPort, basePath)
    migrationClient.loadZKData() // force loading the config to fully set-up the client

    eventually { Await.result(migrationClient.get("test")) }

    Await.result(client1.set("foo", Buf.Utf8("bar")), TIMEOUT)
    val Buf.Utf8(res) = Await.result(client1.get("foo"), TIMEOUT).get
    assert(res == "bar")
    assert(Await.result(client2.get("foo"), TIMEOUT) == None)

    val Buf.Utf8(res2) = Await.result(migrationClient.get("foo"), TIMEOUT).get
    assert(res2 == "bar")

    val Buf.Utf8(res3) = Await.result(client1.get("foo"), TIMEOUT).get
    assert(res3 == "bar")
    eventually { assert(Await.result(client2.get("foo")).map { case Buf.Utf8(s) => s } == Some("bar")) }
  }
}
