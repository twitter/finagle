package com.twitter.finagle.memcached.integration

import com.twitter.common.application.ShutdownRegistry.ShutdownRegistryImpl
import com.twitter.common.zookeeper.ServerSet.EndpointStatus
import com.twitter.common.zookeeper.testing.ZooKeeperTestServer
import com.twitter.common.zookeeper.{CompoundServerSet, ZooKeeperUtils, ServerSets, ZooKeeperClient}
import com.twitter.conversions.time._
import com.twitter.finagle.Group
import com.twitter.finagle.cacheresolver.{CacheNode, CachePoolConfig, ZookeeperCacheNodeGroup}
import com.twitter.util.{Duration, Stopwatch, TimeoutException}
import java.io.ByteArrayOutputStream
import java.net.InetSocketAddress
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class CacheNodeGroupTest extends FunSuite with BeforeAndAfterEach {
  /**
   * Note: This integration test requires a real Memcached server to run.
   */
  var shutdownRegistry: ShutdownRegistryImpl = null
  var testServers: List[(TestMemcachedServer, EndpointStatus)] = List()

  var serverSet: CompoundServerSet = null
  var zookeeperClient: ZooKeeperClient = null
  val zkPath = "/cache/test/silly-cache"
  var zookeeperServer: ZooKeeperTestServer = null

  override def beforeEach() {
    // start zookeeper server and create zookeeper client
    shutdownRegistry = new ShutdownRegistryImpl
    zookeeperServer = new ZooKeeperTestServer(0, shutdownRegistry)
    zookeeperServer.startNetwork()

    // connect to zookeeper server
    zookeeperClient = zookeeperServer.createClient(ZooKeeperClient.digestCredentials("user","pass"))

    // create serverset
    serverSet = new CompoundServerSet(List(
      ServerSets.create(zookeeperClient, ZooKeeperUtils.EVERYONE_READ_CREATOR_ALL, zkPath)))

    // start five memcached server and join the cluster
    addShards(List(0, 1, 2, 3, 4))

    // set cache pool config node data
    val cachePoolConfig: CachePoolConfig = new CachePoolConfig(cachePoolSize = 5)
    val output: ByteArrayOutputStream = new ByteArrayOutputStream
    CachePoolConfig.jsonCodec.serialize(cachePoolConfig, output)
    zookeeperClient.get().setData(zkPath, output.toByteArray, -1)

    // a separate client which only does zk discovery for integration test
    zookeeperClient = zookeeperServer.createClient(ZooKeeperClient.digestCredentials("user","pass"))
  }

  override def afterEach() {
    // shutdown zookeeper server and client
    shutdownRegistry.execute()

    // shutdown memcached server
    testServers foreach { case (s, _) => s.stop() }
    testServers = List()
  }

  if (!sys.props.contains("SKIP_FLAKY")) // CSL-1735
  test("doesn't blow up") {
    val myPool = new ZookeeperCacheNodeGroup(zkPath, zookeeperClient)
    assert(waitForMemberSize(myPool, 0, 5))
    assert(myPool.members forall(_.key.isDefined))
  }

  if (!Option(System.getProperty("SKIP_FLAKY")).isDefined) test("add and remove") {
    // the cluster initially must have 5 members
    val myPool = new ZookeeperCacheNodeGroup(zkPath, zookeeperClient)
    assert(waitForMemberSize(myPool, 0, 5))
    var currentMembers = myPool.members

    /***** start 5 more memcached servers and join the cluster ******/
    // cache pool should remain the same size at this moment
    addShards(List(5, 6, 7, 8, 9))
    assert(waitForMemberSize(myPool, 5, 5))
    assert(myPool.members == currentMembers)

    // update config data node, which triggers the pool update
    // cache pool cluster should be updated
    updateCachePoolConfigData(10)
    assert(waitForMemberSize(myPool, 5, 10))
    assert(myPool.members != currentMembers)
    currentMembers = myPool.members

    /***** remove 2 servers from the zk serverset ******/
    // cache pool should remain the same size at this moment
    testServers(0)._2.leave()
    testServers(1)._2.leave()
    assert(waitForMemberSize(myPool, 10, 10))
    assert(myPool.members == currentMembers)

    // update config data node, which triggers the pool update
    // cache pool should be updated
    updateCachePoolConfigData(8)
    assert(waitForMemberSize(myPool, 10, 8))
    assert(myPool.members != currentMembers)
    currentMembers = myPool.members

    /***** remove 2 more then add 3 ******/
    // cache pool should remain the same size at this moment
    testServers(2)._2.leave()
    testServers(3)._2.leave()
    addShards(List(10, 11, 12))
    assert(waitForMemberSize(myPool, 8, 8))
    assert(myPool.members == currentMembers)

    // update config data node, which triggers the pool update
    // cache pool should be updated
    updateCachePoolConfigData(9)
    assert(waitForMemberSize(myPool, 8, 9))
    assert(myPool.members != currentMembers)
    currentMembers = myPool.members
  }

  if (!Option(System.getProperty("SKIP_FLAKY")).isDefined) test("node key remap") {
    // turn on detecting key remapping
    val output: ByteArrayOutputStream = new ByteArrayOutputStream
    CachePoolConfig.jsonCodec.serialize(CachePoolConfig(5, detectKeyRemapping = true), output)
    zookeeperClient.get().setData(zkPath, output.toByteArray, -1)

    // the cluster initially must have 5 members
    val myPool = new ZookeeperCacheNodeGroup(zkPath, zookeeperClient)
    assert(waitForMemberSize(myPool, 0, 5))
    var currentMembers = myPool.members

    /***** only remap shard key should immediately take effect ******/
    testServers(2)._2.leave()
    testServers(3)._2.leave()
    addShards(List(2, 3))
    assert(waitForMemberSize(myPool, 5, 5))
    assert(myPool.members != currentMembers, myPool.members + " should NOT equal to " + currentMembers)
    currentMembers = myPool.members

    // turn off detecting key remapping
    CachePoolConfig.jsonCodec.serialize(CachePoolConfig(5, detectKeyRemapping = false), output)
    zookeeperClient.get().setData(zkPath, output.toByteArray, -1)
    assert(waitForMemberSize(myPool, 5, 5))
    assert(myPool.members == currentMembers, myPool.members + " should NOT equal to " + currentMembers)
    testServers(4)._2.leave()
    addShards(List(4))
    assert(waitForMemberSize(myPool, 5, 5))
    assert(myPool.members == currentMembers, myPool.members + " should equal to " + currentMembers)

    /***** remap shard key while adding keys should not take effect ******/
    CachePoolConfig.jsonCodec.serialize(CachePoolConfig(5, detectKeyRemapping = true), output)
    zookeeperClient.get().setData(zkPath, output.toByteArray, -1)
    assert(waitForMemberSize(myPool, 5, 5))
    testServers(0)._2.leave()
    testServers(1)._2.leave()
    addShards(List(5, 0, 1))
    assert(waitForMemberSize(myPool, 5, 5))
    assert(myPool.members == currentMembers, myPool.members + " should equal to " + currentMembers)
  }

  if (!Option(System.getProperty("SKIP_FLAKY")).isDefined) test("zk failures test") {
    // the cluster initially must have 5 members
    val myPool = new ZookeeperCacheNodeGroup(zkPath, zookeeperClient)
    assert(waitForMemberSize(myPool, 0, 5))
    var currentMembers = myPool.members

    /***** fail the server here to verify the pool manager will re-establish ******/
    // cache pool cluster should remain the same
    zookeeperServer.expireClientSession(zookeeperClient)
    zookeeperServer.shutdownNetwork()
    assert(waitForMemberSize(myPool, 5, 5))
    assert(myPool.members == currentMembers)

    /***** start the server now ******/
    // cache pool cluster should remain the same
    zookeeperServer.startNetwork
    assert(waitForMemberSize(myPool, 5, 5, 15.seconds))
    assert(myPool.members == currentMembers)

    /***** start 5 more memcached servers and join the cluster ******/
    // update config data node, which triggers the pool update
    // cache pool cluster should still be able to see undelrying pool changes
    addShards(List(5, 6, 7, 8, 9))
    updateCachePoolConfigData(10)
    assert(waitForMemberSize(myPool, 5, 10, 5.seconds))
    assert(myPool.members != currentMembers)
    currentMembers = myPool.members
  }

  private def waitForMemberSize(pool: Group[CacheNode], current: Int, expect: Int, timeout: Duration = 15.seconds): Boolean = {
    val elapsed = Stopwatch.start()
    def loop(): Boolean = {
      if (current != expect && pool.members.size == expect)
        true // expect pool size changes
      else if (current == expect && pool.members.size != expect)
        false // expect pool size remains
      else if (timeout < elapsed()) {
        if (current != expect) throw new TimeoutException("timed out waiting for CacheNode pool to reach the expected size")
        else true
      }
      else {
        Thread.sleep(100)
        loop()
      }
    }
    loop()
  }

  private def updateCachePoolConfigData(size: Int) {
    val cachePoolConfig: CachePoolConfig = new CachePoolConfig(cachePoolSize = size)
    val output: ByteArrayOutputStream = new ByteArrayOutputStream
    CachePoolConfig.jsonCodec.serialize(cachePoolConfig, output)
    zookeeperClient.get().setData(zkPath, output.toByteArray, -1)
  }

  // create temporary zk clients for additional cache servers since we will need to
  private def addShards(shardIds: List[Int]): Unit = {
    shardIds.foreach { shardId =>
      TestMemcachedServer.start() match {
        case Some(server) =>
          testServers :+= (server, serverSet.join(server.address, Map[String, InetSocketAddress](), shardId))
        case None => fail("Cannot start memcached. Skipping...")
      }
    }
  }
}
