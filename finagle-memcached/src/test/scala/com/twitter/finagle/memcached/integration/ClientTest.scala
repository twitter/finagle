package com.twitter.finagle.memcached.integration

import _root_.java.io.ByteArrayOutputStream
import _root_.java.lang.{Boolean => JBoolean}
import java.net.{SocketAddress, InetSocketAddress}
import com.twitter.common.application.ShutdownRegistry.ShutdownRegistryImpl
import com.twitter.common.zookeeper.{ZooKeeperUtils, ServerSets, ZooKeeperClient}
import com.twitter.common.zookeeper.ServerSet.EndpointStatus
import com.twitter.common.zookeeper.testing.ZooKeeperTestServer
import com.twitter.concurrent.Spool
import com.twitter.concurrent.Spool.*::
import com.twitter.conversions.time._
import com.twitter.io.Charsets
import com.twitter.finagle.{Group, MemcachedClient, Name, Resolver, WriteException}
import com.twitter.finagle.builder.{Cluster, ClientBuilder}
import com.twitter.finagle.memcached.{CacheNode, CacheNodeGroup, CachePoolCluster, CachePoolConfig,
  Client, KetamaClientBuilder, PartitionedClient}
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.memcached.protocol.text.Memcached
import com.twitter.finagle.memcached.replication._
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.stats.SummarizingStatsReceiver
import com.twitter.finagle.zookeeper.ZookeeperServerSetCluster
import com.twitter.util.{Await, Duration, Future, Return, Throw}
import scala.collection.mutable
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite, Suites}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ClientTest extends Suites(
  new SimpleClientTest,
  new KetamaClientTest,
  new ClusterClientTest,
  new Finagle6APITest
)

class SimpleClientTest extends FunSuite with BeforeAndAfter {
  /**
    * Note: This integration test requires a real Memcached server to run.
    */
  var client: Client = null
  var testServer: Option[TestMemcachedServer] = None

  val stats = new SummarizingStatsReceiver

  before {
    testServer = TestMemcachedServer.start()
    if (testServer.isDefined) {
      val service = ClientBuilder()
        .hosts(Seq(testServer.get.address))
        .codec(new Memcached(stats))
        .hostConnectionLimit(1)
        .build()
      client = Client(service)
    }
  }

  after {
    if (testServer.isDefined)
      testServer map { _.stop() }
  }

  override def withFixture(test: NoArgTest) {
    if (testServer.isDefined) test()
    else info("Cannot start memcached. Skipping test...")
  }

  test("set & get") {
    Await.result(client.delete("foo"))
    assert(Await.result(client.get("foo")) === None)
    Await.result(client.set("foo", "bar"))
    assert(Await.result(client.get("foo")).get.toString(Charsets.Utf8) === "bar")
  }

  test("get") {
    Await.result(client.set("foo", "bar"))
    Await.result(client.set("baz", "boing"))
    val result = Await.result(client.get(Seq("foo", "baz", "notthere")))
      .map { case (key, value) => (key, value.toString(Charsets.Utf8)) }
    assert(result === Map(
      "foo" -> "bar",
      "baz" -> "boing"
    ))
  }

  if (Option(System.getProperty("USE_EXTERNAL_MEMCACHED")).isDefined) {
    test("gets") {
      Await.result(client.set("foos", "xyz"))
      Await.result(client.set("bazs", "xyz"))
      Await.result(client.set("bazs", "zyx"))
      val result = Await.result(client.gets(Seq("foos", "bazs", "somethingelse")))
        .map { case (key, (value, casUnique)) =>
          (key, (value.toString(Charsets.Utf8), casUnique.toString(Charsets.Utf8)))
      }

      assert(result === Map(
        "foos" -> ("xyz", "1"),  // the "cas unique" values are predictable from a fresh memcached
        "bazs" -> ("zyx", "3")
      ))
    }
  }

  if (Option(System.getProperty("USE_EXTERNAL_MEMCACHED")).isDefined) {
    test("cas") {
      Await.result(client.set("x", "y"))
      val Some((value, casUnique)) = Await.result(client.gets("x"))
      assert(value.toString(Charsets.Utf8) === "y")
      assert(casUnique.toString(Charsets.Utf8) === "1")

      assert(!Await.result(client.cas("x", "z", "2")))
      assert(Await.result(client.cas("x", "z", casUnique)))
      val res = Await.result(client.get("x"))
      assert(res.isDefined)
      assert(res.get.toString(Charsets.Utf8) === "z")
    }
  }

  test("append & prepend") {
    Await.result(client.set("foo", "bar"))
    Await.result(client.append("foo", "rab"))
    assert(Await.result(client.get("foo")).get.toString(Charsets.Utf8) === "barrab")
    Await.result(client.prepend("foo", "rab"))
    assert(Await.result(client.get("foo")).get.toString(Charsets.Utf8) === "rabbarrab")
  }

  test("incr & decr") {
    // As of memcached 1.4.8 (issue 221), empty values are no longer treated as integers
    Await.result(client.set("foo", "0"))
    assert(Await.result(client.incr("foo"))    === Some(1L))
    assert(Await.result(client.incr("foo", 2)) === Some(3L))
    assert(Await.result(client.decr("foo"))    === Some(2L))

    Await.result(client.set("foo", "0"))
    assert(Await.result(client.incr("foo"))    === Some(1L))
    val l = 1L << 50
    assert(Await.result(client.incr("foo", l)) === Some(l + 1L))
    assert(Await.result(client.decr("foo"))    === Some(l))
    assert(Await.result(client.decr("foo", l)) === Some(0L))
  }

  if (Option(System.getProperty("USE_EXTERNAL_MEMCACHED")).isDefined) {
    test("stats") {
      val stats = Await.result(client.stats())
      assert(stats != null)
      assert(!stats.isEmpty)
      stats.foreach { stat =>
        assert(stat.startsWith("STAT"))
      }
    }
  }

  test("send malformed keys") {
    // test key validation trait
    intercept[ClientError] { Await.result(client.get("fo o")) }
    intercept[ClientError] { Await.result(client.set("", "bar")) }
    intercept[ClientError] { Await.result(client.get("    foo")) }
    intercept[ClientError] { Await.result(client.get("foo   ")) }
    intercept[ClientError] { Await.result(client.get("    foo")) }
    val nullString: String = null
    intercept[ClientError] { Await.result(client.get(nullString)) }
    intercept[ClientError] { Await.result(client.set(nullString, "bar")) }
    intercept[ClientError] { Await.result(client.set("    ", "bar")) }

    try { Await.result(client.set("\t", "bar")) }
    catch { case _: ClientError => fail("\t is allowed") }

    intercept[ClientError] { Await.result(client.set("\r", "bar")) }
    intercept[ClientError] { Await.result(client.set("\n", "bar")) }
    intercept[ClientError] { Await.result(client.set("\0", "bar")) }

    val veryLongKey = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
    intercept[ClientError] { Await.result(client.get(veryLongKey)) }
    assert(Await.ready(client.set(veryLongKey, "bar")).poll.get.isThrow)

    // test other keyed command validation
    val nullSeq:Seq[String] = null
   intercept[ClientError] { Await.result(client.get(nullSeq)) }
   intercept[ClientError] { Await.result(client.gets(nullSeq)) }
   intercept[ClientError] { Await.result(client.gets(Seq(null))) }
   intercept[ClientError] { Await.result(client.gets(Seq(""))) }
   intercept[ClientError] { Await.result(client.gets(Seq("foos", "bad key", "somethingelse"))) }
   intercept[ClientError] { Await.result(client.append("bad key", "rab")) }
   intercept[ClientError] { Await.result(client.prepend("bad key", "rab")) }
   intercept[ClientError] { Await.result(client.replace("bad key", "bar")) }
   intercept[ClientError] { Await.result(client.add("bad key", "2")) }
   intercept[ClientError] { Await.result(client.cas("bad key", "z", "2")) }
   intercept[ClientError] { Await.result(client.incr("bad key")) }
   intercept[ClientError] { Await.result(client.decr("bad key")) }
   intercept[ClientError] { Await.result(client.delete("bad key")) }
  }
}

class KetamaClientTest extends FunSuite with BeforeAndAfter {

  /**
    * We already proved above that we can hit a real memcache server,
    * so we can use our own for the partitioned client test.
    */
  var server1: InProcessMemcached = null
  var server2: InProcessMemcached = null
  var address1: InetSocketAddress = null
  var address2: InetSocketAddress = null

  before {
    server1 = new InProcessMemcached(new InetSocketAddress(0))
    address1 = server1.start().localAddress.asInstanceOf[InetSocketAddress]
    server2 = new InProcessMemcached(new InetSocketAddress(0))
    address2 = server2.start().localAddress.asInstanceOf[InetSocketAddress]
  }

  after {
    server1.stop()
    server2.stop()
  }

  test("doesn't blow up") {
    val client = KetamaClientBuilder()
      .nodes("localhost:%d,localhost:%d".format(address1.getPort, address2.getPort))
      .build()

    Await.result(client.delete("foo"))
    assert(Await.result(client.get("foo")) === None)
    Await.result(client.set("foo", "bar"))
    assert(Await.result(client.get("foo")).get.toString(Charsets.Utf8) === "bar")
  }

  test("using Name doesn't blow up") {
    val name = Name.bound(address1, address2)
    val client = KetamaClientBuilder().dest(name).build()

    Await.result(client.delete("foo"))
    assert(Await.result(client.get("foo")) === None)
    Await.result(client.set("foo", "bar"))
    assert(Await.result(client.get("foo")).get.toString(Charsets.Utf8) === "bar")
  }

  test("using Group[InetSocketAddress] doesn't blow up") {
    val mutableGroup = Group(address1, address2).map{_.asInstanceOf[SocketAddress]}
    val client = KetamaClientBuilder()
      .group(CacheNodeGroup(mutableGroup, true))
      .build()

    Await.result(client.delete("foo"))
    assert(Await.result(client.get("foo")) === None)
    Await.result(client.set("foo", "bar"))
    assert(Await.result(client.get("foo")).get.toString(Charsets.Utf8) === "bar")
  }

  test("using custom keys doesn't blow up") {
    val client = KetamaClientBuilder()
      .nodes("localhost:%d:1:key1,localhost:%d:1:key2".format(address1.getPort, address2.getPort))
      .build()

    Await.result(client.delete("foo"))
    assert(Await.result(client.get("foo")) === None)
    Await.result(client.set("foo", "bar"))
    assert(Await.result(client.get("foo")).get.toString(Charsets.Utf8) === "bar")
  }

  test("even in future pool") {
    lazy val client = KetamaClientBuilder()
      .nodes("localhost:%d,localhost:%d".format(address1.getPort, address2.getPort))
      .build()

    val futureResult = Future.value(true) flatMap {
      _ => client.get("foo")
    }

    assert(Await.result(futureResult) === None)
  }
}

class ClusterClientTest extends FunSuite with BeforeAndAfter {

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

  before {
    // start zookeeper server and create zookeeper client
    shutdownRegistry = new ShutdownRegistryImpl
    zookeeperServer = new ZooKeeperTestServer(0, shutdownRegistry)
    zookeeperServer.startNetwork()

    // connect to zookeeper server
    zookeeperClient = zookeeperServer.createClient(ZooKeeperClient.digestCredentials("user","pass"))

    // create serverset
    val serverSet = ServerSets.create(zookeeperClient, ZooKeeperUtils.EVERYONE_READ_CREATOR_ALL, zkPath)
    zkServerSetCluster = new ZookeeperServerSetCluster(serverSet)

    // start five memcached server and join the cluster
    (0 to 4) foreach { _ =>
      TestMemcachedServer.start() match  {
        case Some(server) =>
          testServers :+= server
          zkServerSetCluster.join(server.address)
      }
    }

    if (!testServers.isEmpty) {
      // set cache pool config node data
      val cachePoolConfig: CachePoolConfig = new CachePoolConfig(cachePoolSize = 5)
      val output: ByteArrayOutputStream = new ByteArrayOutputStream
      CachePoolConfig.jsonCodec.serialize(cachePoolConfig, output)
      zookeeperClient.get().setData(zkPath, output.toByteArray, -1)

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

  override def withFixture(test: NoArgTest) {
    if (!testServers.isEmpty) test()
    else info("Cannot start memcached. Skipping test...")
  }

  test("Simple ClusterClient using finagle load balancing - many keys") {
    // create simple cluster client
    val mycluster =
      new ZookeeperServerSetCluster(
        ServerSets.create(zookeeperClient, ZooKeeperUtils.EVERYONE_READ_CREATOR_ALL, zkPath))
    Await.result(mycluster.ready) // give it sometime for the cluster to get the initial set of memberships
    val client = Client(mycluster)

    val count = 100
      (0 until count).foreach{
        n => {
          client.set("foo"+n, "bar"+n)
        }
      }

    val tmpClients = testServers map {
      case(server) =>
        Client(
          ClientBuilder()
            .hosts(server.address)
            .codec(new Memcached)
            .hostConnectionLimit(1)
            .build())
    }

    (0 until count).foreach {
      n => {
        var found = false
        tmpClients foreach {
          c =>
          if (Await.result(c.get("foo"+n))!=None) {
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

  test("Ketama ClusterClient using a distributor - set & get") {
    val client = KetamaClientBuilder()
      .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
      .failureAccrualParams(Int.MaxValue, Duration.Top)
      .dest(dest)
      .build()

    client.delete("foo")()
    assert(client.get("foo")() === None)
    client.set("foo", "bar")()
    assert(client.get("foo")().get.toString(Charsets.Utf8) === "bar")
  }

  test("Ketama ClusterClient using a distributor - many keys") {
    val client = KetamaClientBuilder()
      .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
      .failureAccrualParams(Int.MaxValue, Duration.Top)
      .dest(dest)
      .build()
      .asInstanceOf[PartitionedClient]

    val count = 100
      (0 until count).foreach{
        n => {
          client.set("foo"+n, "bar"+n)()
        }
      }

    (0 until count).foreach {
      n => {
        val c = client.clientOf("foo"+n)
        assert(c.get("foo"+n)().get.toString(Charsets.Utf8) === "bar"+n)
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

    assert(trackCacheShards(client.asInstanceOf[PartitionedClient]).size === 5)
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
      assert(trackCacheShards(client).size === 5)

      // add 4 more cache servers and update cache pool config data, now there should be 7 shards
      var additionalServers = List[EndpointStatus]()
      try {
        expectPoolStatus(mycluster, currentSize = 5, expectedPoolSize = 9, expectedAdd = 4, expectedRem = 0) {
          additionalServers = addMoreServers(4)
          updateCachePoolConfigData(9)
        }.get(10.seconds)()
      }
      catch { case _: Exception => fail("it shouldn't trown an exception") }

      // Unlike CachePoolCluster, our KetamaClient doesn't have api to expose its internal state and
      // it shouldn't, hence here I don't really have a better way to wait for the client's key ring
      // redistribution to finish other than sleep for a while.
      Thread.sleep(1000)
      assert(trackCacheShards(client).size === 9)

      // remove 2 cache servers and update cache pool config data, now there should be 7 shards
      try {
        expectPoolStatus(mycluster, currentSize = 9, expectedPoolSize = 7, expectedAdd = 0, expectedRem = 2) {
          additionalServers(0).leave()
          additionalServers(1).leave()
          updateCachePoolConfigData(7)
        }.get(10.seconds)()
      }
      catch { case _: Exception => fail("it shouldn't trown an exception") }

      Thread.sleep(1000)
      assert(trackCacheShards(client).size === 7)

      // remove another 2 cache servers and update cache pool config data, now there should be 5 shards
      try {
        expectPoolStatus(mycluster, currentSize = 7, expectedPoolSize = 5, expectedAdd = 0, expectedRem = 2) {
          additionalServers(2).leave()
          additionalServers(3).leave()
          updateCachePoolConfigData(5)
        }.get(10.seconds)()
      }
      catch { case _: Exception => fail("it shouldn't trown an exception") }

      Thread.sleep(1000)
      assert(trackCacheShards(client).size === 5)

      // add 2 more cache servers and update cache pool config data, now there should be 7 shards
      try {
        expectPoolStatus(mycluster, currentSize = 5, expectedPoolSize = 7, expectedAdd = 2, expectedRem = 0) {
          additionalServers = addMoreServers(2)
          updateCachePoolConfigData(7)
        }.get(10.seconds)()
      }
      catch { case _: Exception => fail("it shouldn't trown an exception") }

      Thread.sleep(1000)
      assert(trackCacheShards(client).size === 7)

      // add another 2 more cache servers and update cache pool config data, now there should be 9 shards
      try {
        expectPoolStatus(mycluster, currentSize = 7, expectedPoolSize = 9, expectedAdd = 2, expectedRem = 0) {
          additionalServers = addMoreServers(2)
          updateCachePoolConfigData(9)
        }.get(10.seconds)()
      }
      catch { case _: Exception => fail("it shouldn't trown an exception") }

      Thread.sleep(1000)
      assert(trackCacheShards(client).size === 9)

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

      Thread.sleep(1000)
      assert(trackCacheShards(client).size === 9)
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
      assert(trackCacheShards(client).size === 5)

      // add 4 more cache servers and update cache pool config data, now there should be 7 shards
      var additionalServers = List[EndpointStatus]()
      try {
        expectPoolStatus(mycluster, currentSize = 5, expectedPoolSize = 9, expectedAdd = 4, expectedRem = 0) {
          additionalServers = addMoreServers(4)
        }.get(10.seconds)()
      }
      catch { case _: Exception => fail("it shouldn't trown an exception") }

      // Unlike CachePoolCluster, our KetamaClient doesn't have api to expose its internal state and
      // it shouldn't, hence here I don't really have a better way to wait for the client's key ring
      // redistribution to finish other than sleep for a while.
      Thread.sleep(1000)
      assert(trackCacheShards(client).size === 9)

      // remove 2 cache servers and update cache pool config data, now there should be 7 shards
      try {
        expectPoolStatus(mycluster, currentSize = 9, expectedPoolSize = 7, expectedAdd = 0, expectedRem = 2) {
          additionalServers(0).leave()
          additionalServers(1).leave()
        }.get(10.seconds)()
      }
      catch { case _: Exception => fail("it shouldn't trown an exception") }

      Thread.sleep(1000)
      assert(trackCacheShards(client).size === 7)
    }

  def updateCachePoolConfigData(size: Int) {
    val cachePoolConfig: CachePoolConfig = new CachePoolConfig(cachePoolSize = size)
    var output: ByteArrayOutputStream = new ByteArrayOutputStream
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

    Await.result(myCachePool.ready) // wait until the pool is ready
    myCachePool.snap match {
      case (cachePool, changes) =>
        assert(cachePool.size === expectedSize)
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
        assert(cachePool.size === currentSize)
        poolSeen ++= cachePool
        val retFuture = changes flatMap expectMore
        ops // invoke the function now
        retFuture
    }
  }

  def trackCacheShards(client: PartitionedClient) = mutable.Set.empty[Client] ++
  ((0 until 100).map { n => client.clientOf("foo"+n) })

}

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

  override def withFixture(test: NoArgTest) {
    if (!testServers.isEmpty) test()
    else info("Cannot start memcached. Skipping test...")
  }

  if (!Option(System.getProperty("SKIP_FLAKY")).isDefined) {
    test("with unmanaged regular zk serverset") {
      val client = MemcachedClient.newKetamaClient(
        "zk!localhost:"+zookeeperServerPort+"!"+zkPath).asInstanceOf[PartitionedClient]

      // Wait for group to contain members
      Thread.sleep(5000)

      val count = 100
        (0 until count).foreach{
          n => {
            client.set("foo"+n, "bar"+n)()
          }
        }

      (0 until count).foreach {
        n => {
          val c = client.clientOf("foo"+n)
          assert(c.get("foo"+n)().get.toString(Charsets.Utf8) === "bar"+n)
        }
      }
    }
  }

  if (!Option(System.getProperty("SKIP_FLAKY")).isDefined)
    test("with managed cache pool") {
      val client = MemcachedClient.newKetamaClient(
        "twcache!localhost:"+zookeeperServerPort+"!"+zkPath).asInstanceOf[PartitionedClient]

      // Wait for group to contain members
      Thread.sleep(5000)

      client.delete("foo")()
      assert(client.get("foo")() === None)
      client.set("foo", "bar")()
      assert(client.get("foo")().get.toString(Charsets.Utf8) === "bar")

      val count = 100
        (0 until count).foreach{
          n => {
            client.set("foo"+n, "bar"+n)()
          }
        }

      (0 until count).foreach {
        n => {
          val c = client.clientOf("foo"+n)
          assert(c.get("foo"+n)().get.toString(Charsets.Utf8) === "bar"+n)
        }
      }
    }

  test("with static servers list") {
    val client = MemcachedClient.newKetamaClient(
      "twcache!localhost:%d,localhost:%d".format(testServers(0).address.getPort, testServers(1).address.getPort))

    Await.result(client.delete("foo"))
    assert(Await.result(client.get("foo")) === None)
    Await.result(client.set("foo", "bar"))
    assert(Await.result(client.get("foo")).get.toString(Charsets.Utf8) === "bar")
  }
}
