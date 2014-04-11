package com.twitter.finagle.memcached.integration

import com.twitter.common.io.FileUtils._
import com.twitter.common.quantity.{Time, Amount}
import com.twitter.common.zookeeper.{ZooKeeperUtils, ServerSets, ZooKeeperClient}
import com.twitter.conversions.time._
import com.twitter.finagle.MemcachedClient
import com.twitter.finagle.memcached.CachePoolConfig
import com.twitter.finagle.memcached.migration._
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.zookeeper.ZookeeperServerSetCluster
import com.twitter.util._
import java.io.ByteArrayOutputStream
import org.apache.zookeeper.server.persistence.FileTxnSnapLog
import org.apache.zookeeper.server.{NIOServerCnxn, ZooKeeperServer}
import org.jboss.netty.util.CharsetUtil
import org.junit.runner.RunWith
import org.scalatest.concurrent.{IntegrationPatience, Eventually}
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
  var connectionFactory: NIOServerCnxn.Factory = null

  var testServers: List[TestMemcachedServer] = List()

  override def beforeEach() {
    val zookeeperAddress = RandomSocket.nextAddress
    zookeeperServerPort = zookeeperAddress.getPort

    // start zookeeper server and create zookeeper client
    zookeeperServer = new ZooKeeperServer(
      new FileTxnSnapLog(createTempDir(), createTempDir()),
      new ZooKeeperServer.BasicDataTreeBuilder)
    connectionFactory = new NIOServerCnxn.Factory(zookeeperAddress)
    connectionFactory.startup(zookeeperServer)
    zookeeperClient = new ZooKeeperClient(
      Amount.of(10, Time.MILLISECONDS),
      zookeeperAddress)

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

  def waitForEventualResult[A](op: () => A, result: A, timeout: Duration = 5.seconds): Boolean = {
    val elapsed = Stopwatch.start()
    def loop(): Boolean = {
      val res = op()
      if (res.equals(result))
        true
      else if (timeout < elapsed())
        false
      else {
        Thread.sleep(1000)
        loop()
      }
    }
    loop()
  }

  if (!sys.props.contains("SKIP_FLAKY")) {
    test("not migrating yet") {
      val client1 = MemcachedClient.newKetamaClient(
        dest = "twcache!localhost:"+zookeeperServerPort+"!"+oldPoolPath)
      val client2 = MemcachedClient.newKetamaClient(
        dest = "twcache!localhost:"+zookeeperServerPort+"!"+newPoolPath)
      val migrationClient = MigrationClient.newMigrationClient("localhost:"+zookeeperServerPort, basePath)
      migrationClient.loadZKData() // force loading the config to fully set-up the client

      eventually { Await.result(migrationClient.get("test")) }

      assert(Await.result(migrationClient.get("foo")) == None)
      Await.result(migrationClient.set("foo", "bar"))
      assert(Await.result(migrationClient.get("foo")).get.toString(CharsetUtil.UTF_8) == "bar")

      assert(Await.result(client1.get("foo")).get.toString(CharsetUtil.UTF_8) == "bar")
      assert(waitForEventualResult(() => Await.result(client2.get("foo")), None))
    }

    test("sending dark traffic") {
      val migrationConfig = MigrationConstants.MigrationConfig("Warming", false, false)
      val migrationDataArray = MigrationConstants.jsonMapper.writeValueAsString(migrationConfig)
      zookeeperClient.get().setData(basePath, migrationDataArray, -1)

      val client1 = MemcachedClient.newKetamaClient(
        dest = "twcache!localhost:"+zookeeperServerPort+"!"+oldPoolPath)
      val client2 = MemcachedClient.newKetamaClient(
        dest = "twcache!localhost:"+zookeeperServerPort+"!"+newPoolPath)
      val migrationClient = MigrationClient.newMigrationClient("localhost:"+zookeeperServerPort, basePath)
      migrationClient.loadZKData() // force loading the config to fully set-up the client

      eventually { Await.result(migrationClient.get("test")) }

      assert(Await.result(migrationClient.get("foo")) == None)
      Await.result(migrationClient.set("foo", "bar"))
      assert(Await.result(migrationClient.get("foo")).get.toString(CharsetUtil.UTF_8) == "bar")

      assert(Await.result(client1.get("foo")).get.toString(CharsetUtil.UTF_8) == "bar")
      assert(waitForEventualResult(() => Await.result(client2.get("foo")).map(_.toString(CharsetUtil.UTF_8)), Some("bar")))
    }

    test("dark read w/ read repair") {
      val migrationConfig = MigrationConstants.MigrationConfig("Warming", true, false)
      val migrationDataArray = MigrationConstants.jsonMapper.writeValueAsString(migrationConfig)
      zookeeperClient.get().setData(basePath, migrationDataArray, -1)

      val client1 = MemcachedClient.newKetamaClient(
        dest = "twcache!localhost:"+zookeeperServerPort+"!"+oldPoolPath)
      val client2 = MemcachedClient.newKetamaClient(
        dest = "twcache!localhost:"+zookeeperServerPort+"!"+newPoolPath)
      val migrationClient = MigrationClient.newMigrationClient("localhost:"+zookeeperServerPort, basePath)
      migrationClient.loadZKData() // force loading the config to fully set-up the client

      eventually { Await.result(migrationClient.get("test")) }

      Await.result(client1.set("foo", "bar"))
      assert(Await.result(client1.get("foo")).get.toString(CharsetUtil.UTF_8) == "bar")
      assert(Await.result(client2.get("foo")) == None)

      assert(Await.result(migrationClient.get("foo")).get.toString(CharsetUtil.UTF_8) == "bar")

      assert(Await.result(client1.get("foo")).get.toString(CharsetUtil.UTF_8) == "bar")
      assert(waitForEventualResult(() => Await.result(client2.get("foo")).map(_.toString(CharsetUtil.UTF_8)), Some("bar")))
    }

    test("use new pool with fallback to old pool") {
      val migrationConfig = MigrationConstants.MigrationConfig("Verifying", false, false)
      val migrationDataArray = MigrationConstants.jsonMapper.writeValueAsString(migrationConfig)
      zookeeperClient.get().setData(basePath, migrationDataArray, -1)

      val client1 = MemcachedClient.newKetamaClient(
        dest = "twcache!localhost:"+zookeeperServerPort+"!"+oldPoolPath)
      val client2 = MemcachedClient.newKetamaClient(
        dest = "twcache!localhost:"+zookeeperServerPort+"!"+newPoolPath)
      val migrationClient = MigrationClient.newMigrationClient("localhost:"+zookeeperServerPort, basePath)
      migrationClient.loadZKData() // force loading the config to fully set-up the client

      eventually { Await.result(migrationClient.get("test")) }

      Await.result(client1.set("foo", "bar"))
      assert(Await.result(client1.get("foo")).get.toString(CharsetUtil.UTF_8) == "bar")
      assert(Await.result(client2.get("foo")) == None)

      assert(Await.result(migrationClient.get("foo")).get.toString(CharsetUtil.UTF_8) == "bar")

      assert(Await.result(client1.get("foo")).get.toString(CharsetUtil.UTF_8) == "bar")
      assert(waitForEventualResult(() => Await.result(client2.get("foo")), None))
    }

    test("use new pool with fallback to old pool and readrepair") {
      val migrationConfig = MigrationConstants.MigrationConfig("Verifying", false, true)
      val migrationDataArray = MigrationConstants.jsonMapper.writeValueAsString(migrationConfig)
      zookeeperClient.get().setData(basePath, migrationDataArray, -1)

      val client1 = MemcachedClient.newKetamaClient(
        dest = "twcache!localhost:"+zookeeperServerPort+"!"+oldPoolPath)
      val client2 = MemcachedClient.newKetamaClient(
        dest = "twcache!localhost:"+zookeeperServerPort+"!"+newPoolPath)
      val migrationClient = MigrationClient.newMigrationClient("localhost:"+zookeeperServerPort, basePath)
      migrationClient.loadZKData() // force loading the config to fully set-up the client

      eventually { Await.result(migrationClient.get("test")) }

      Await.result(client1.set("foo", "bar"))
      assert(Await.result(client1.get("foo")).get.toString(CharsetUtil.UTF_8) == "bar")
      assert(Await.result(client2.get("foo")) == None)

      assert(Await.result(migrationClient.get("foo")).get.toString(CharsetUtil.UTF_8) == "bar")

      assert(Await.result(client1.get("foo")).get.toString(CharsetUtil.UTF_8) == "bar")
      assert(waitForEventualResult(() => Await.result(client2.get("foo")).map(_.toString(CharsetUtil.UTF_8)), Some("bar")))
    }
  }
}
