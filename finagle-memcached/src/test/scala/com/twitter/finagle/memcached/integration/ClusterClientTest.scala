package com.twitter.finagle.memcached.integration

import _root_.java.io.ByteArrayOutputStream
import com.twitter.common.application.ShutdownRegistry.ShutdownRegistryImpl
import com.twitter.common.zookeeper.ServerSet.EndpointStatus
import com.twitter.common.zookeeper.testing.ZooKeeperTestServer
import com.twitter.common.zookeeper.{ServerSets, ZooKeeperClient, ZooKeeperUtils}
import com.twitter.concurrent.Spool
import com.twitter.concurrent.Spool.*::
import com.twitter.conversions.time._
import com.twitter.finagle.builder.{ClientBuilder, Cluster}
import com.twitter.finagle.cacheresolver.{CachePoolConfig, CacheNode, CachePoolCluster}
import com.twitter.finagle.memcached.protocol.text.Memcached
import com.twitter.finagle.memcached.{Client, KetamaClientBuilder, PartitionedClient}
import com.twitter.finagle.zookeeper.ZookeeperServerSetCluster
import com.twitter.finagle.{Name, Resolver}
import com.twitter.io.Buf
import com.twitter.util.{FuturePool, Await, Duration, Future}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite, Outcome}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.junit.JUnitRunner
import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class ClusterClientTest
  extends FunSuite
  with BeforeAndAfter
  with Eventually
  with IntegrationPatience {

  /**
   * Note: This integration test requires a real Memcached server to run.
   */
  var shutdownRegistry: ShutdownRegistryImpl = null
  var testServers: List[TestMemcachedServer] = List()

  var zkServerSetCluster: ZookeeperServerSetCluster = null
  var zookeeperClient: ZooKeeperClient = null
  val zkPath = "/cache/test/silly-cache"
  var zookeeperServer: ZooKeeperTestServer = null
  var dest: Name = null
  val pool = FuturePool.unboundedPool

  val TimeOut = 15.seconds
  def boundedWait[T](body: => T): T = Await.result(pool { body }, TimeOut)

  before {
    // start zookeeper server and create zookeeper client
    shutdownRegistry = new ShutdownRegistryImpl
    zookeeperServer = new ZooKeeperTestServer(0, shutdownRegistry)
    zookeeperServer.startNetwork()

    // connect to zookeeper server
    zookeeperClient = boundedWait(
      zookeeperServer.createClient(ZooKeeperClient.digestCredentials("user","pass"))
    )

    // create serverset
    val serverSet = boundedWait(
      ServerSets.create(zookeeperClient, ZooKeeperUtils.EVERYONE_READ_CREATOR_ALL, zkPath)
    )
    zkServerSetCluster = new ZookeeperServerSetCluster(serverSet)

    // start five memcached server and join the cluster
    Await.result(
      Future.collect(
        (0 to 4) map { _ =>
          TestMemcachedServer.start() match {
            case Some(server) =>
              testServers :+= server
              pool { zkServerSetCluster.join(server.address) }
            case None =>
              fail("could not start TestMemcachedServer")
          }
        }
      )
    , TimeOut)

    if (!testServers.isEmpty) {
      // set cache pool config node data
      val cachePoolConfig: CachePoolConfig = new CachePoolConfig(cachePoolSize = 5)
      val output: ByteArrayOutputStream = new ByteArrayOutputStream
      CachePoolConfig.jsonCodec.serialize(cachePoolConfig, output)
      boundedWait(zookeeperClient.get().setData(zkPath, output.toByteArray, -1))

      // a separate client which only does zk discovery for integration test
      zookeeperClient = zookeeperServer.createClient(ZooKeeperClient.digestCredentials("user","pass"))

      // destination of the test cache endpoints
      dest = Resolver.eval("twcache!localhost:" + zookeeperServer.getPort.toString + "!" + zkPath)
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

  test("Simple ClusterClient using finagle load balancing - many keys") {
    // create simple cluster client
    val mycluster =
      new ZookeeperServerSetCluster(
        ServerSets.create(zookeeperClient, ZooKeeperUtils.EVERYONE_READ_CREATOR_ALL, zkPath))
    Await.result(mycluster.ready, TimeOut) // give it sometime for the cluster to get the initial set of memberships
    val client = Client(mycluster)

    val count = 100
    Await.result(
      Future.collect((0 until count) map { n =>
        client.set("foo"+n, Buf.Utf8("bar"+n))
      })
    , TimeOut)

    val tmpClients = testServers map {
      case(server) =>
        Client(
          ClientBuilder()
            .hosts(server.address)
            .codec(new Memcached)
            .hostConnectionLimit(1)
            .daemon(true)
            .build())
    }

    (0 until count).foreach {
      n => {
        var found = false
        tmpClients foreach {
          c =>
          if (Await.result(c.get("foo"+n), TimeOut)!=None) {
            assert(!found)
            found = true
          }
        }
        assert(found)
      }
    }
  }

  if (!sys.props.contains("SKIP_FLAKY"))
    test("Cache specific cluster - add and remove") {

      // the cluster initially must have 5 members
      val myPool = initializePool(5)

      var additionalServers = List[EndpointStatus]()

      /***** start 5 more memcached servers and join the cluster ******/
      // cache pool should remain the same size at this moment
      intercept[com.twitter.util.TimeoutException] {
        expectPoolStatus(myPool, currentSize = 5, expectedPoolSize = -1, expectedAdd = -1, expectedRem = -1) {
          additionalServers = addMoreServers(5)
        }.get(2.seconds)()
      }

      // update config data node, which triggers the pool update
      // cache pool cluster should be updated
      try {
        expectPoolStatus(myPool, currentSize = 5, expectedPoolSize = 10, expectedAdd = 5, expectedRem = 0) {
          updateCachePoolConfigData(10)
        }.get(10.seconds)()
      }
      catch { case _: Exception => fail("it shouldn't trown an exception") }

      /***** remove 2 servers from the zk serverset ******/
      // cache pool should remain the same size at this moment
      intercept[com.twitter.util.TimeoutException] {
        expectPoolStatus(myPool, currentSize = 10, expectedPoolSize = -1, expectedAdd = -1, expectedRem = -1) {
          additionalServers(0).leave()
          additionalServers(1).leave()
        }.get(2.seconds)()
      }

      // update config data node, which triggers the pool update
      // cache pool should be updated
      try {
        expectPoolStatus(myPool, currentSize = 10, expectedPoolSize = 8, expectedAdd = 0, expectedRem = 2) {
          updateCachePoolConfigData(8)
        }.get(10.seconds)()
      }
      catch { case _: Exception => fail("it shouldn't trown an exception") }

      /***** remove 2 more then add 3 ******/
      // cache pool should remain the same size at this moment
      intercept[com.twitter.util.TimeoutException] {
        expectPoolStatus(myPool, currentSize = 8, expectedPoolSize = -1, expectedAdd = -1, expectedRem = -1) {
          additionalServers(2).leave()
          additionalServers(3).leave()
          addMoreServers(3)
        }.get(2.seconds)()
      }

      // update config data node, which triggers the pool update
      // cache pool should be updated
      try {
        expectPoolStatus(myPool, currentSize = 8, expectedPoolSize = 9, expectedAdd = 3, expectedRem = 2) {
          updateCachePoolConfigData(9)
        }.get(10.seconds)()
      }
      catch { case _: Exception => fail("it shouldn't trown an exception") }
    }


  if (!Option(System.getProperty("SKIP_FLAKY")).isDefined)
    test("zk failures test") {
      // the cluster initially must have 5 members
      val myPool = initializePool(5)

      /***** fail the server here to verify the pool manager will re-establish ******/
      // cache pool cluster should remain the same
      intercept[com.twitter.util.TimeoutException] {
        expectPoolStatus(myPool, currentSize = 5, expectedPoolSize = -1, expectedAdd = -1, expectedRem = -1) {
          zookeeperServer.expireClientSession(zookeeperClient)
          zookeeperServer.shutdownNetwork()
        }.get(2.seconds)()
      }

      /***** start the server now ******/
      // cache pool cluster should remain the same
      intercept[com.twitter.util.TimeoutException] {
        expectPoolStatus(myPool, currentSize = 5, expectedPoolSize = -1, expectedAdd = -1, expectedRem = -1) {
          zookeeperServer.startNetwork
          Thread.sleep(2000)
        }.get(2.seconds)()
      }

      /***** start 5 more memcached servers and join the cluster ******/
      // update config data node, which triggers the pool update
      // cache pool cluster should still be able to see undelrying pool changes
      try {
        expectPoolStatus(myPool, currentSize = 5, expectedPoolSize = 10, expectedAdd = 5, expectedRem = 0) {
          addMoreServers(5)
          updateCachePoolConfigData(10)
        }.get(10.seconds)()
      }
      catch { case _: Exception => fail("it shouldn't trown an exception") }
    }

  if (!Option(System.getProperty("SKIP_FLAKY")).isDefined)
    test("using backup pools") {
      // shutdown the server before initializing our cache pool cluster
      zookeeperServer.shutdownNetwork()

      // the cache pool cluster should pickup backup pools
      // the underlying pool will continue trying to connect to zk
      val myPool = initializePool(2, Some(scala.collection.immutable.Set(
        new CacheNode("host1", 11211, 1),
        new CacheNode("host2", 11212, 1))))

      // bring the server back online
      // give it some time we should see the cache pool cluster pick up underlying pool
      try {
        expectPoolStatus(myPool, currentSize = 2, expectedPoolSize = 5, expectedAdd = 5, expectedRem = 2) {
          zookeeperServer.startNetwork
        }.get(10.seconds)()
      }
      catch { case _: Exception => fail("it shouldn't trown an exception") }

      /***** start 5 more memcached servers and join the cluster ******/
      // update config data node, which triggers the pool update
      // cache pool cluster should still be able to see undelrying pool changes
      try {
        expectPoolStatus(myPool, currentSize = 5, expectedPoolSize = 10, expectedAdd = 5, expectedRem = 0) {
          addMoreServers(5)
          updateCachePoolConfigData(10)
        }.get(10.seconds)()
      }
      catch { case _: Exception => fail("it shouldn't trown an exception") }
    }

  if (!Option(System.getProperty("SKIP_FLAKY")).isDefined) {
    test("Ketama ClusterClient using a distributor - set & get") {
      val client = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .failureAccrualParams(Int.MaxValue, Duration.Top)
        .dest(dest)
        .build()

      Await.result(client.delete("foo"), TimeOut)
      assert(Await.result(client.get("foo"), TimeOut) == None)
      Await.result(client.set("foo", Buf.Utf8("bar")), TimeOut)
      val Buf.Utf8(res) = Await.result(client.get("foo"), TimeOut).get
      assert(res == "bar")
    }
  }

  if (!Option(System.getProperty("SKIP_FLAKY")).isDefined) {
    test("Ketama ClusterClient using a distributor - many keys") {
      val client = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .failureAccrualParams(Int.MaxValue, Duration.Top)
        .dest(dest)
        .build()
        .asInstanceOf[PartitionedClient]

      val count = 100
      Await.result(Future.collect(
        (0 until count) map { n => client.set("foo" + n, Buf.Utf8("bar" + n))}
      ), TimeOut)

      (0 until count).foreach {
        n => {
          val c = client.clientOf("foo" + n)
          val Buf.Utf8(res) = Await.result(c.get("foo" + n), TimeOut).get
          assert(res == "bar" + n)
        }
      }
    }
  }

  test("Ketama ClusterClient using a distributor - use custom keys") {
    // create my cluster client solely based on a zk client and a path
    val mycluster = CachePoolCluster.newZkCluster(zkPath, zookeeperClient)
    mycluster.ready() // give it sometime for the cluster to get the initial set of memberships

    val customKey = "key-"
    var shardId = -1
    val myClusterWithCustomKey = mycluster map {
      case node: CacheNode => {
        shardId += 1
        CacheNode(node.host, node.port, node.weight, Some(customKey + shardId.toString))
      }
    }
    val client = KetamaClientBuilder()
      .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
      .failureAccrualParams(Int.MaxValue, Duration.Top)
      .cachePoolCluster(myClusterWithCustomKey)
      .build()

    assert(trackCacheShards(client.asInstanceOf[PartitionedClient]).size == 5)
  }

  if (!Option(System.getProperty("SKIP_FLAKY")).isDefined)
    test("Ketama ClusterClient using a distributor - cache pool is changing") {
      // create my cluster client solely based on a zk client and a path
      val mycluster = initializePool(5)

      val client = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .failureAccrualParams(Int.MaxValue, Duration.Top)
        .cachePoolCluster(mycluster)
        .build()
        .asInstanceOf[PartitionedClient]

      // initially there should be 5 cache shards being used
      assert(trackCacheShards(client).size == 5)

      // add 4 more cache servers and update cache pool config data, now there should be 7 shards
      var additionalServers = List[EndpointStatus]()
      try {
        expectPoolStatus(mycluster, currentSize = 5, expectedPoolSize = 9, expectedAdd = 4, expectedRem = 0) {
          additionalServers = addMoreServers(4)
          updateCachePoolConfigData(9)
        }.get(10.seconds)()
      }
      catch { case _: Exception => fail("it shouldn't trown an exception") }

      eventually { assert(trackCacheShards(client).size == 9) }

      // remove 2 cache servers and update cache pool config data, now there should be 7 shards
      try {
        expectPoolStatus(mycluster, currentSize = 9, expectedPoolSize = 7, expectedAdd = 0, expectedRem = 2) {
          additionalServers(0).leave()
          additionalServers(1).leave()
          updateCachePoolConfigData(7)
        }.get(10.seconds)()
      }
      catch { case _: Exception => fail("it shouldn't trown an exception") }

      eventually { assert(trackCacheShards(client).size == 7) }

      // remove another 2 cache servers and update cache pool config data, now there should be 5 shards
      try {
        expectPoolStatus(mycluster, currentSize = 7, expectedPoolSize = 5, expectedAdd = 0, expectedRem = 2) {
          additionalServers(2).leave()
          additionalServers(3).leave()
          updateCachePoolConfigData(5)
        }.get(10.seconds)()
      }
      catch { case _: Exception => fail("it shouldn't trown an exception") }

      eventually { assert(trackCacheShards(client).size == 5) }

      // add 2 more cache servers and update cache pool config data, now there should be 7 shards
      try {
        expectPoolStatus(mycluster, currentSize = 5, expectedPoolSize = 7, expectedAdd = 2, expectedRem = 0) {
          additionalServers = addMoreServers(2)
          updateCachePoolConfigData(7)
        }.get(10.seconds)()
      }
      catch { case _: Exception => fail("it shouldn't trown an exception") }

      eventually { assert(trackCacheShards(client).size == 7) }

      // add another 2 more cache servers and update cache pool config data, now there should be 9 shards
      try {
        expectPoolStatus(mycluster, currentSize = 7, expectedPoolSize = 9, expectedAdd = 2, expectedRem = 0) {
          additionalServers = addMoreServers(2)
          updateCachePoolConfigData(9)
        }.get(10.seconds)()
      }
      catch { case _: Exception => fail("it shouldn't trown an exception") }

      eventually { assert(trackCacheShards(client).size == 9) }

      // remove 2 and add 2, now there should be still 9 shards
      try {
        expectPoolStatus(mycluster, currentSize = 9, expectedPoolSize = 9, expectedAdd = 2, expectedRem = 2) {
          additionalServers(0).leave()
          additionalServers(1).leave()
          addMoreServers(2)
          updateCachePoolConfigData(9)
        }.get(10.seconds)()
      }
      catch { case _: Exception => fail("it shouldn't trown an exception") }

      eventually { assert(trackCacheShards(client).size == 9) }
    }

  if (!Option(System.getProperty("SKIP_FLAKY")).isDefined)
    test("Ketama ClusterClient using a distributor - unmanaged cache pool is changing") {
      // create my cluster client solely based on a zk client and a path
      val mycluster = initializePool(5, ignoreConfigData = true)

      val client = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .failureAccrualParams(Int.MaxValue, Duration.Top)
        .cachePoolCluster(mycluster)
        .build()
        .asInstanceOf[PartitionedClient]

      // initially there should be 5 cache shards being used
      assert(trackCacheShards(client).size == 5)

      // add 4 more cache servers and update cache pool config data, now there should be 7 shards
      var additionalServers = List[EndpointStatus]()
      try {
        expectPoolStatus(mycluster, currentSize = 5, expectedPoolSize = 9, expectedAdd = 4, expectedRem = 0) {
          additionalServers = addMoreServers(4)
        }.get(10.seconds)()
      }
      catch { case _: Exception => fail("it shouldn't trown an exception") }

      eventually { assert(trackCacheShards(client).size == 9) }

      // remove 2 cache servers and update cache pool config data, now there should be 7 shards
      try {
        expectPoolStatus(mycluster, currentSize = 9, expectedPoolSize = 7, expectedAdd = 0, expectedRem = 2) {
          additionalServers(0).leave()
          additionalServers(1).leave()
        }.get(10.seconds)()
      }
      catch { case _: Exception => fail("it shouldn't trown an exception") }

      eventually { assert(trackCacheShards(client).size == 7) }
    }

  def updateCachePoolConfigData(size: Int) {
    val cachePoolConfig: CachePoolConfig = new CachePoolConfig(cachePoolSize = size)
    val output = new ByteArrayOutputStream
    CachePoolConfig.jsonCodec.serialize(cachePoolConfig, output)
    zookeeperClient.get().setData(zkPath, output.toByteArray, -1)
  }

  // create temporary zk clients for additional cache servers since we will need to
  // de-register these services by expiring corresponding zk client session
  def addMoreServers(size: Int): List[EndpointStatus] = {
    (1 to size) map { _ =>
      val server = TestMemcachedServer.start()
      testServers :+= server.get
      zkServerSetCluster.joinServerSet(server.get.address)
    } toList
  }

  def initializePool(
    expectedSize: Int,
    backupPool: Option[scala.collection.immutable.Set[CacheNode]]=None,
    ignoreConfigData: Boolean = false
  ): Cluster[CacheNode] = {
    val myCachePool =
      if (! ignoreConfigData) CachePoolCluster.newZkCluster(zkPath, zookeeperClient, backupPool = backupPool)
      else CachePoolCluster.newUnmanagedZkCluster(zkPath, zookeeperClient)

    Await.result(myCachePool.ready, TimeOut) // wait until the pool is ready
    myCachePool.snap match {
      case (cachePool, changes) =>
        assert(cachePool.size == expectedSize)
    }
    myCachePool
  }

  /**
    * return a future which will be complete only if the cache pool changed AND the changes
    * meet all expected conditions after executing operation passed in
    * @param currentSize expected current pool size
    * @param expectedPoolSize expected pool size after changes, use -1 to expect any size
    * @param expectedAdd expected number of add event happened, use -1 to expect any number
    * @param expectedRem expected number of rem event happened, use -1 to expect any number
    * @param ops operation to execute
    */
  def expectPoolStatus(
    myCachePool: Cluster[CacheNode],
    currentSize: Int,
    expectedPoolSize: Int,
    expectedAdd: Int,
    expectedRem: Int
  )(ops: => Unit): Future[Unit] = {
    var addSeen = 0
    var remSeen = 0
    var poolSeen = mutable.HashSet[CacheNode]()

    def expectMore(spoolChanges: Spool[Cluster.Change[CacheNode]]): Future[Unit] = {
      spoolChanges match {
        case change *:: tail =>
          change match {
            case Cluster.Add(node) =>
              addSeen += 1
              poolSeen.add(node)
            case Cluster.Rem(node) =>
              remSeen += 1
              poolSeen.remove(node)
          }
          if ((expectedAdd == -1 || addSeen == expectedAdd) &&
            (expectedRem == -1 || remSeen == expectedRem) &&
            (expectedPoolSize == -1 || poolSeen.size == expectedPoolSize)) Future.Done
          else tail flatMap expectMore
      }
    }

    myCachePool.snap match {
      case (cachePool, changes) =>
        assert(cachePool.size == currentSize)
        poolSeen ++= cachePool
        val retFuture = changes flatMap expectMore
        ops // invoke the function now
        retFuture
    }
  }

  def trackCacheShards(client: PartitionedClient) = mutable.Set.empty[Client] ++
  ((0 until 100).map { n => client.clientOf("foo"+n) })
}
