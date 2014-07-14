package com.twitter.finagle.memcached.integration

import java.io.ByteArrayOutputStream
import java.lang.{Boolean => JBoolean, UnsupportedOperationException}
import com.twitter.common.application.ShutdownRegistry.ShutdownRegistryImpl
import com.twitter.common.zookeeper.testing.ZooKeeperTestServer
import com.twitter.common.zookeeper.{ZooKeeperUtils, ServerSets, ZooKeeperClient}
import com.twitter.conversions.time._
import com.twitter.finagle.{Group, WriteException}
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.memcached.protocol.text.Memcached
import com.twitter.finagle.memcached.replication._
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.memcached.{CachePoolCluster, CachePoolConfig, KetamaClientBuilder}
import com.twitter.finagle.zookeeper.ZookeeperServerSetCluster
import com.twitter.util.{Await, Return, Throw}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ReplicationClientTest extends FunSuite with BeforeAndAfterEach {
  /**
   * Note: This integration test requires a real Memcached server to run.
   */
  var shutdownRegistry: ShutdownRegistryImpl = null
  var firstTestServerPool = List[TestMemcachedServer]()
  var secondTestServerPool = List[TestMemcachedServer]()

  val firstPoolPath = "/cache/test/silly-cache-1"
  val secondPoolPath = "/cache/test/silly-cache-2"
  var zookeeperServer: ZooKeeperTestServer = null
  var zookeeperClient: ZooKeeperClient = null

  override def beforeEach() {
    // start zookeeper server and create zookeeper client
    shutdownRegistry = new ShutdownRegistryImpl
    zookeeperServer = new ZooKeeperTestServer(0, shutdownRegistry)
    zookeeperServer.startNetwork()

    // connect to zookeeper server
    zookeeperClient = zookeeperServer.createClient(ZooKeeperClient.digestCredentials("user","pass"))

    // start two memcached server and join the cluster
    val firstPoolCluster = new ZookeeperServerSetCluster(
      ServerSets.create(zookeeperClient, ZooKeeperUtils.EVERYONE_READ_CREATOR_ALL, firstPoolPath))
    (0 to 1) foreach { _ =>
      TestMemcachedServer.start() match {
        case Some(server) =>
          firstTestServerPool :+= server
          firstPoolCluster.join(server.address)
        case None => fail("Cannot start memcached.")
      }
    }

    val secondPoolCluster = new ZookeeperServerSetCluster(
      ServerSets.create(zookeeperClient, ZooKeeperUtils.EVERYONE_READ_CREATOR_ALL, secondPoolPath))
    (0 to 1) foreach { _ =>
      TestMemcachedServer.start() match {
        case Some(server) =>
          secondTestServerPool :+= server
          secondPoolCluster.join(server.address)
        case None => fail("Cannot start memcached.")
      }
    }

    // set cache pool config node data
    val cachePoolConfig: CachePoolConfig = new CachePoolConfig(cachePoolSize = 2)
    val output: ByteArrayOutputStream = new ByteArrayOutputStream
    CachePoolConfig.jsonCodec.serialize(cachePoolConfig, output)
    zookeeperClient.get().setData(firstPoolPath, output.toByteArray, -1)
    zookeeperClient.get().setData(secondPoolPath, output.toByteArray, -1)

    // a separate client which only does zk discovery for integration test
    zookeeperClient = zookeeperServer.createClient(ZooKeeperClient.digestCredentials("user","pass"))
  }

  override def afterEach() {
    // shutdown zookeeper server and client
    shutdownRegistry.execute()

    // shutdown memcached server
    firstTestServerPool foreach { _.stop() }
    secondTestServerPool foreach { _.stop() }
    firstTestServerPool = List()
    secondTestServerPool = List()
  }

  test("base replication client set & getOne") {
    // create my cluster client solely based on a zk client and a path
    val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
    Await.result(mycluster1.ready) // give it sometime for the cluster to get the initial set of memberships
    val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
    Await.result(mycluster2.ready) // give it sometime for the cluster to get the initial set of memberships

    val client1 = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .group(Group.fromCluster(mycluster1))
        .failureAccrualParams(Int.MaxValue, 0.seconds)
        .build()
    val client2 = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .group(Group.fromCluster(mycluster2))
        .failureAccrualParams(Int.MaxValue, 0.seconds)
        .build()
    val replicatedClient = new BaseReplicationClient(Seq(client1, client2))

    // consistent
    assert(Await.result(replicatedClient.getOne("foo")) == None)
    assert(Await.result(replicatedClient.set("foo", "bar")) == ConsistentReplication(()))
    assert(Await.result(replicatedClient.getOne("foo")) == Some(stringToChannelBuffer("bar")))

    // inconsistent data

    Await.result(client2.set("client2-only", "test"))
    assert(Await.result(replicatedClient.getOne("client2-only")) == Some(stringToChannelBuffer("test")))

    // inconsistent replica state
    firstTestServerPool(0).stop()
    firstTestServerPool(1).stop()
    assert(Await.result(replicatedClient.set("foo", "baz")) match {
      case InconsistentReplication(Seq(Throw(_), Return(()))) => true
      case _ => false
    })
    assert(Await.result(replicatedClient.getOne("foo")) == Some(stringToChannelBuffer("baz")))

    // all failed
    secondTestServerPool(0).stop()
    secondTestServerPool(1).stop()
    assert(Await.result(replicatedClient.set("foo", "baz")) match {
      case FailedReplication(Seq(Throw(_), Throw(_))) => true
      case _ => false
    })
    intercept[WriteException] {
      Await.result(replicatedClient.getOne("foo"))
    }
  }

  test("base replication client set & getAll") {
    // create my cluster client solely based on a zk client and a path
    val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
    Await.result(mycluster1.ready) // give it sometime for the cluster to get the initial set of memberships
    val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
    Await.result(mycluster2.ready) // give it sometime for the cluster to get the initial set of memberships

    val client1 = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .group(Group.fromCluster(mycluster1))
        .failureAccrualParams(Int.MaxValue, 0.seconds)
        .build()
    val client2 = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .group(Group.fromCluster(mycluster2))
        .failureAccrualParams(Int.MaxValue, 0.seconds)
        .build()
    val replicatedClient = new BaseReplicationClient(Seq(client1, client2))

    // consistent
    assert(Await.result(replicatedClient.getAll("foo")) == ConsistentReplication(None))
    assert(Await.result(replicatedClient.set("foo", "bar")) == ConsistentReplication(()))
    assert(Await.result(replicatedClient.getAll("foo")) == ConsistentReplication(
      Some(stringToChannelBuffer("bar"))))

    // inconsistent data
    Await.result(client2.set("client2-only", "test"))
    assert(Await.result(replicatedClient.getAll("client2-only")) == InconsistentReplication(
      Seq(Return(None), Return(Some(stringToChannelBuffer("test"))))))

    // inconsistent replica state
    firstTestServerPool(0).stop()
    firstTestServerPool(1).stop()
    assert(Await.result(replicatedClient.set("foo", "baz")) match {
      case InconsistentReplication(Seq(Throw(_), Return(()))) => true
      case _ => false
    })
    assert(Await.result(replicatedClient.getAll("foo")) match {
      case InconsistentReplication(Seq(Throw(_), Return(Some(v)))) =>
        v equals stringToChannelBuffer("baz")
      case _ => false
    })

    // all failed
    secondTestServerPool(0).stop()
    secondTestServerPool(1).stop()
    assert(Await.result(replicatedClient.set("foo", "baz")) match {
      case FailedReplication(Seq(Throw(_), Throw(_))) => true
      case _ => false
    })
    assert(Await.result(replicatedClient.getAll("foo")) match {
      case FailedReplication(Seq(Throw(_), Throw(_))) => true
      case _ => false
    })
  }

  test("base replication client delete") {
    // create my cluster client solely based on a zk client and a path
    val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
    Await.result(mycluster1.ready) // give it sometime for the cluster to get the initial set of memberships
    val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
    Await.result(mycluster2.ready) // give it sometime for the cluster to get the initial set of memberships

    val client1 = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .group(Group.fromCluster(mycluster1))
        .failureAccrualParams(Int.MaxValue, 0.seconds)
        .build()
    val client2 = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .group(Group.fromCluster(mycluster2))
        .failureAccrualParams(Int.MaxValue, 0.seconds)
        .build()
    val replicatedClient = new BaseReplicationClient(Seq(client1, client2))

    // consistent
    assert(Await.result(replicatedClient.delete("empty-key")) == ConsistentReplication(false))

    assert(Await.result(replicatedClient.set("foo", "bar")) == ConsistentReplication(()))
    assert(Await.result(replicatedClient.getAll("foo")) == ConsistentReplication(
      Some(stringToChannelBuffer("bar"))))
    assert(Await.result(replicatedClient.delete("foo")) == ConsistentReplication(true))

    // inconsistent data
    assert(Await.result(client2.add("client2-only", "bar")) == true)
    assert(Await.result(replicatedClient.delete("client2-only")) match {
      case InconsistentReplication(Seq(Return(JBoolean.FALSE), Return(JBoolean.TRUE))) => true
      case _ => false
    })

    // inconsistent replica state
    Await.result(client2.set("client2-only", "bar"))
    firstTestServerPool(0).stop()
    firstTestServerPool(1).stop()
    assert(Await.result(replicatedClient.delete("client2-only")) match {
      case InconsistentReplication(Seq(Throw(_), Return(JBoolean.TRUE))) => true
      case _ => false
    })

    // all failed
    secondTestServerPool(0).stop()
    secondTestServerPool(1).stop()
    assert(Await.result(replicatedClient.delete("client2-only")) match {
      case FailedReplication(Seq(Throw(_), Throw(_))) => true
      case _ => false
    })
  }

  if (Option(System.getProperty("USE_EXTERNAL_MEMCACHED")).isDefined) test("base replication client getsAll & cas") {
    // create my cluster client solely based on a zk client and a path
    val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
    Await.result(mycluster1.ready) // give it sometime for the cluster to get the initial set of memberships
    val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
    Await.result(mycluster2.ready) // give it sometime for the cluster to get the initial set of memberships

    val client1 = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .group(Group.fromCluster(mycluster1))
        .failureAccrualParams(Int.MaxValue, 0.seconds)
        .build()
    val client2 = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .group(Group.fromCluster(mycluster2))
        .failureAccrualParams(Int.MaxValue, 0.seconds)
        .build()
    val replicatedClient = new BaseReplicationClient(Seq(client1, client2))

    // consistent
    assert(Await.result(replicatedClient.set("foo", "bar")) == ConsistentReplication(()))
    assert(Await.result(replicatedClient.getsAll("foo")) == ConsistentReplication(
      Some((stringToChannelBuffer("bar"), RCasUnique(Seq("1", "1"))))))
    Await.result(client1.set("foo", "bar"))
    assert(Await.result(replicatedClient.getsAll("foo")) == ConsistentReplication(
      Some((stringToChannelBuffer("bar"), RCasUnique(Seq("2", "1"))))))
    assert(Await.result(replicatedClient.cas("foo", "baz", Seq("2", "1"))) == ConsistentReplication(true))
    assert(Await.result(replicatedClient.cas("foo", "baz", Seq("3", "2"))) == ConsistentReplication(true))
    Await.result(client1.set("foo", "bar"))
    Await.result(client2.set("foo", "bar"))
    assert(Await.result(replicatedClient.cas("foo", "baz", Seq("4", "3"))) == ConsistentReplication(false))
    assert(Await.result(replicatedClient.delete("foo")) == ConsistentReplication(true))
    assert(Await.result(replicatedClient.getsAll("foo")) == ConsistentReplication(None))

    // inconsistent data
    Await.result(client1.set("foo", "bar"))
    Await.result(client2.set("foo", "baz"))
    assert(Await.result(replicatedClient.getsAll("foo")) == InconsistentReplication(
      Seq(Return(Some(stringToChannelBuffer("bar"), SCasUnique("6"))),
        Return(Some((stringToChannelBuffer("baz"), SCasUnique("5")))))))
    assert(Await.result(client1.delete("foo")) == true)
    assert(Await.result(replicatedClient.getsAll("foo")) == InconsistentReplication(
      Seq(Return(None), Return(Some((stringToChannelBuffer("baz"), SCasUnique("5")))))))
    assert(Await.result(replicatedClient.cas("foo", "bar", Seq("7", "5"))) match {
      case InconsistentReplication(Seq(Throw(_), Return(JBoolean.TRUE))) => true
      case _ => false
    })
    Await.result(client1.set("foo", "bar"))
    assert(Await.result(replicatedClient.cas("foo", "bar", Seq("6", "6"))) == InconsistentReplication(
      Seq(Return(false), Return(true))))

    // inconsistent replica state
    firstTestServerPool(0).stop()
    firstTestServerPool(1).stop()
    assert(Await.result(replicatedClient.getsAll("foo")) match {
      case InconsistentReplication(Seq(Throw(_), Return(Some((v, SCasUnique(_)))))) =>
        v equals stringToChannelBuffer("bar")
      case _ => false
    })
    assert(Await.result(replicatedClient.cas("foo", "bar", Seq("7", "7"))) match {
      case InconsistentReplication(Seq(Throw(_), Return(JBoolean.TRUE))) => true
      case _ => false
    })

    // all failed
    secondTestServerPool(0).stop()
    secondTestServerPool(1).stop()
    assert(Await.result(replicatedClient.getsAll("foo")) match {
      case FailedReplication(Seq(Throw(_), Throw(_))) => true
      case _ => false
    })
    assert(Await.result(replicatedClient.cas("foo", "bar", Seq("7", "7"))) match {
      case FailedReplication(Seq(Throw(_), Throw(_))) => true
      case _ => false
    })
  }

  test("base replication client add & replace") {
    // create my cluster client solely based on a zk client and a path
    val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
    Await.result(mycluster1.ready) // give it sometime for the cluster to get the initial set of memberships
    val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
    Await.result(mycluster2.ready) // give it sometime for the cluster to get the initial set of memberships

    val client1 = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .group(Group.fromCluster(mycluster1))
        .failureAccrualParams(Int.MaxValue, 0.seconds)
        .build()
    val client2 = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .group(Group.fromCluster(mycluster2))
        .failureAccrualParams(Int.MaxValue, 0.seconds)
        .build()
    val replicatedClient = new BaseReplicationClient(Seq(client1, client2))

    // consistent
    assert(Await.result(replicatedClient.add("foo", "bar")) == ConsistentReplication(true))
    assert(Await.result(replicatedClient.getAll("foo")) == ConsistentReplication(
      Some(stringToChannelBuffer("bar"))))

    assert(Await.result(replicatedClient.replace("foo", "baz")) == ConsistentReplication(true))
    assert(Await.result(replicatedClient.getAll("foo")) == ConsistentReplication(
      Some(stringToChannelBuffer("baz"))))

    assert(Await.result(replicatedClient.add("foo", "bar")) == ConsistentReplication(false))
    assert(Await.result(replicatedClient.replace("no-such-key", "test")) == ConsistentReplication(false))

    // inconsistent data
    assert(Await.result(client1.add("client1-only", "test")) == true)
    assert(Await.result(client2.add("client2-only", "test")) == true)
    assert(Await.result(replicatedClient.add("client2-only", "test")) match {
      case InconsistentReplication(Seq(Return(JBoolean.TRUE), Return(JBoolean.FALSE))) => true
      case _ => false
    })
    assert(Await.result(replicatedClient.replace("client1-only", "test")) match {
      case InconsistentReplication(Seq(Return(JBoolean.TRUE), Return(JBoolean.FALSE))) => true
      case _ => false
    })

    // inconsistent replica state
    firstTestServerPool(0).stop()
    firstTestServerPool(1).stop()
    assert(Await.result(replicatedClient.add("client2-only", "test")) match {
      case InconsistentReplication(Seq(Throw(_), Return(JBoolean.FALSE))) => true
      case _ => false
    })
    assert(Await.result(replicatedClient.replace("client1-only", "test")) match {
      case InconsistentReplication(Seq(Throw(_), Return(JBoolean.FALSE))) => true
      case _ => false
    })

    // all failed
    secondTestServerPool(0).stop()
    secondTestServerPool(1).stop()
    assert(Await.result(replicatedClient.add("client2-only", "test")) match {
      case FailedReplication(Seq(Throw(_), Throw(_))) => true
      case _ => false
    })
    assert(Await.result(replicatedClient.replace("client1-only", "test")) match {
      case FailedReplication(Seq(Throw(_), Throw(_))) => true
      case _ => false
    })
  }

  test("base replication client incr & decr") {
    // create my cluster client solely based on a zk client and a path
    val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
    Await.result(mycluster1.ready) // give it sometime for the cluster to get the initial set of memberships
    val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
    Await.result(mycluster2.ready) // give it sometime for the cluster to get the initial set of memberships

    val client1 = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .group(Group.fromCluster(mycluster1))
        .failureAccrualParams(Int.MaxValue, 0.seconds)
        .build()
    val client2 = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .group(Group.fromCluster(mycluster2))
        .failureAccrualParams(Int.MaxValue, 0.seconds)
        .build()
    val replicatedClient = new BaseReplicationClient(Seq(client1, client2))

    // consistent
    assert(Await.result(replicatedClient.set("foo", "1")) == ConsistentReplication(()))
    assert(Await.result(replicatedClient.getAll("foo")) == ConsistentReplication(
      Some(stringToChannelBuffer("1"))))
    assert(Await.result(replicatedClient.incr("foo", 2)) == ConsistentReplication(Some(3L)))
    assert(Await.result(replicatedClient.getAll("foo")) == ConsistentReplication(
      Some(stringToChannelBuffer("3"))))
    assert(Await.result(replicatedClient.decr("foo", 1)) == ConsistentReplication(Some(2L)))
    assert(Await.result(replicatedClient.getAll("foo")) == ConsistentReplication(
      Some(stringToChannelBuffer("2"))))

    // inconsistent data
    assert(Await.result(client1.incr("foo", 1)) == Some(3L))
    assert(Await.result(replicatedClient.incr("foo", 1)) == InconsistentReplication(
      Seq(Return(Some(4L)), Return(Some(3L)))))
    assert(Await.result(client2.decr("foo", 1)) == Some(2L))
    assert(Await.result(replicatedClient.decr("foo", 1)) == InconsistentReplication(
      Seq(Return(Some(3L)), Return(Some(1L)))))

    assert(Await.result(client1.delete("foo")) == true)
    assert(Await.result(replicatedClient.incr("foo", 1)) == InconsistentReplication(
      Seq(Return(None), Return(Some(2L)))))

    // inconsistent replica state
    firstTestServerPool(0).stop()
    firstTestServerPool(1).stop()
    assert(Await.result(replicatedClient.decr("foo", 1)) match {
      case InconsistentReplication(Seq(Throw(_), Return(Some(v)))) => v equals 1L
      case _ => false
    })

    // all failed
    secondTestServerPool(0).stop()
    secondTestServerPool(1).stop()
    assert(Await.result(replicatedClient.decr("foo", 1)) match {
      case FailedReplication(Seq(Throw(_), Throw(_))) => true
      case _ => false
    })

  }

  test("base replication client many keys") {
    // create my cluster client solely based on a zk client and a path
    val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
    Await.result(mycluster1.ready) // give it sometime for the cluster to get the initial set of memberships
    val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
    Await.result(mycluster2.ready) // give it sometime for the cluster to get the initial set of memberships

    val client1 = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .group(Group.fromCluster(mycluster1))
        .failureAccrualParams(Int.MaxValue, 0.seconds)
        .build()
    val client2 = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .group(Group.fromCluster(mycluster2))
        .failureAccrualParams(Int.MaxValue, 0.seconds)
        .build()
    val replicatedClient = new BaseReplicationClient(Seq(client1, client2))

    val count = 100
    (0 until count).foreach{
      n => {
        Await.result(replicatedClient.set("foo"+n, "bar"+n))
      }
    }

    (0 until count).foreach {
      n => {
        assert(Await.result(replicatedClient.getAll("foo"+n)) == ConsistentReplication(
          Some(stringToChannelBuffer("bar"+n))))
      }
    }

    // shutdown primary pool
    firstTestServerPool(0).stop()
    firstTestServerPool(1).stop()

    (0 until count).foreach {
      n => {
        assert(Await.result(replicatedClient.getAll("foo"+n)) match {
          case InconsistentReplication(Seq(Throw(_), Return(Some(v)))) =>
            v equals stringToChannelBuffer("bar"+n)
          case _ => false
        })
      }
    }
  }

  test("base replication client replica down") {
    // create my cluster client solely based on a zk client and a path
    val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
    Await.result(mycluster1.ready) // give it sometime for the cluster to get the initial set of memberships
    val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
    Await.result(mycluster2.ready) // give it sometime for the cluster to get the initial set of memberships

    val client1 = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .group(Group.fromCluster(mycluster1))
        .failureAccrualParams(Int.MaxValue, 0.seconds)
        .build()
    val client2 = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .group(Group.fromCluster(mycluster2))
        .failureAccrualParams(Int.MaxValue, 0.seconds)
        .build()
    val replicatedClient = new BaseReplicationClient(Seq(client1, client2))

    assert(Await.result(replicatedClient.set("foo", "bar")) == ConsistentReplication(()))
    assert(Await.result(replicatedClient.getAll("foo")) == ConsistentReplication(
      Some(stringToChannelBuffer("bar"))))

    // primary pool down
    firstTestServerPool(0).stop()
    firstTestServerPool(1).stop()

    assert(Await.result(replicatedClient.getAll("foo")) match {
      case InconsistentReplication(Seq(Throw(_), Return(Some(v)))) => v equals stringToChannelBuffer("bar")
      case _ => false
    })
    assert(Await.result(replicatedClient.set("foo", "baz")) match {
      case InconsistentReplication(Seq(Throw(_), Return(()))) => true
      case _ => false
    })

    // bring back primary pool
    TestMemcachedServer.start(Some(firstTestServerPool(0).address))
    TestMemcachedServer.start(Some(firstTestServerPool(1).address))

    assert(Await.result(replicatedClient.getAll("foo")) match {
      case InconsistentReplication(Seq(Return(None), Return(Some(v)))) =>
        v equals stringToChannelBuffer("baz")
      case _ => false
    })
    assert(Await.result(replicatedClient.set("foo", "baz")) == ConsistentReplication(()))
  }

  test("base replication client non supported operation") {
    // create my cluster client solely based on a zk client and a path
    val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
    Await.result(mycluster1.ready) // give it sometime for the cluster to get the initial set of memberships
    val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
    Await.result(mycluster2.ready) // give it sometime for the cluster to get the initial set of memberships

    val client1 = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .group(Group.fromCluster(mycluster1))
        .failureAccrualParams(Int.MaxValue, 0.seconds)
        .build()
    val client2 = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .group(Group.fromCluster(mycluster2))
        .failureAccrualParams(Int.MaxValue, 0.seconds)
        .build()
    val replicatedClient = new BaseReplicationClient(Seq(client1, client2))

    intercept[UnsupportedOperationException] {
      Await.result(replicatedClient.append("not-supported", "value"))
    }
    intercept[UnsupportedOperationException] {
      Await.result(replicatedClient.prepend("not-supported", "value"))
    }

  }

  test("simple replication client get & set") {
    // create my cluster client solely based on a zk client and a path
    val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
    Await.result(mycluster1.ready) // give it sometime for the cluster to get the initial set of memberships
    val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
    Await.result(mycluster2.ready) // give it sometime for the cluster to get the initial set of memberships

    val client1 = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .group(Group.fromCluster(mycluster1))
        .failureAccrualParams(Int.MaxValue, 0.seconds)
        .build()
    val client2 = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .group(Group.fromCluster(mycluster2))
        .failureAccrualParams(Int.MaxValue, 0.seconds)
        .build()
    val replicatedClient = new SimpleReplicationClient(Seq(client1, client2))

    // consistent
    assert(Await.result(replicatedClient.get("foo")) == None)
    Await.result(replicatedClient.set("foo", "bar"))
    assert(Await.result(replicatedClient.get("foo")) == Some(stringToChannelBuffer("bar")))
    assert(Await.result(client1.get("foo")) == Some(stringToChannelBuffer("bar")))
    assert(Await.result(client2.get("foo")) == Some(stringToChannelBuffer("bar")))

    // inconsistent data
    Await.result(client2.set("client2-only", "test"))
    assert(Await.result(client1.get("client2-only")) == None)
    assert(Await.result(client2.get("client2-only")) == Some(stringToChannelBuffer("test")))
    assert(Await.result(replicatedClient.get("client2-only")) == Some(stringToChannelBuffer("test")))

    // set overwrites existing data
    Await.result(replicatedClient.set("client2-only", "test-again"))
    assert(Await.result(replicatedClient.get("client2-only")) == Some(stringToChannelBuffer("test-again")))
    assert(Await.result(client1.get("client2-only")) == Some(stringToChannelBuffer("test-again")))
    assert(Await.result(client1.get("client2-only")) == Some(stringToChannelBuffer("test-again")))

    // inconsistent replica state
    firstTestServerPool(0).stop()
    firstTestServerPool(1).stop()
    intercept[SimpleReplicationFailure] {
      Await.result(replicatedClient.set("foo", "baz"))
    }
    Await.result(replicatedClient.get("foo"))

    secondTestServerPool(0).stop()
    secondTestServerPool(1).stop()
    intercept[SimpleReplicationFailure] {
      Await.result(replicatedClient.set("foo", "baz"))
    }
    intercept[WriteException] {
      Await.result(replicatedClient.get("foo"))
    }
  }

  if (Option(System.getProperty("USE_EXTERNAL_MEMCACHED")).isDefined) test("simple replication client gets & cas") {
    // create my cluster client solely based on a zk client and a path
    val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
    Await.result(mycluster1.ready) // give it sometime for the cluster to get the initial set of memberships
    val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
    Await.result(mycluster2.ready) // give it sometime for the cluster to get the initial set of memberships

    val client1 = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .group(Group.fromCluster(mycluster1))
        .failureAccrualParams(Int.MaxValue, 0.seconds)
        .build()
    val client2 = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .group(Group.fromCluster(mycluster2))
        .failureAccrualParams(Int.MaxValue, 0.seconds)
        .build()
    val replicatedClient = new SimpleReplicationClient(Seq(client1, client2))

    // consistent
    assert(Await.result(replicatedClient.gets("foo")) == None)
    Await.result(replicatedClient.set("foo", "bar"))
    assert(Await.result(replicatedClient.gets("foo")) == Some(
      (stringToChannelBuffer("bar"), stringToChannelBuffer("1|1"))))

    // inconsistent data
    Await.result(client1.set("inconsistent-key", "client1"))
    Await.result(client2.set("inconsistent-key", "client2"))
    assert(Await.result(replicatedClient.gets("inconsistent-key")) == None)

    // cas overwrites existing data
    assert(Await.result(replicatedClient.cas("foo", "baz", stringToChannelBuffer("1|1"))) == true)

    // inconsistent replica state
    firstTestServerPool(0).stop()
    firstTestServerPool(1).stop()
    intercept[SimpleReplicationFailure] {
      Await.result(replicatedClient.cas("foo", "baz", stringToChannelBuffer("2|3")))
    }
    intercept[SimpleReplicationFailure] {
      Await.result(replicatedClient.gets("foo"))
    }
  }

  test("simple replication client delete") {
    // create my cluster client solely based on a zk client and a path
    val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
    Await.result(mycluster1.ready) // give it sometime for the cluster to get the initial set of memberships
    val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
    Await.result(mycluster2.ready) // give it sometime for the cluster to get the initial set of memberships

    val client1 = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .group(Group.fromCluster(mycluster1))
        .failureAccrualParams(Int.MaxValue, 0.seconds)
        .build()
    val client2 = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .group(Group.fromCluster(mycluster2))
        .failureAccrualParams(Int.MaxValue, 0.seconds)
        .build()
    val replicatedClient = new SimpleReplicationClient(Seq(client1, client2))

    // consistent
    assert(Await.result(replicatedClient.delete("empty-key")) == false)

    Await.result(replicatedClient.set("foo", "bar"))
    assert(Await.result(replicatedClient.get("foo")) == Some(stringToChannelBuffer("bar")))
    assert(Await.result(client1.get("foo")) == Some(stringToChannelBuffer("bar")))
    assert(Await.result(client1.get("foo")) == Some(stringToChannelBuffer("bar")))
    assert(Await.result(replicatedClient.delete("foo")) == true)
    assert(Await.result(client1.get("foo")) == None)
    assert(Await.result(client2.get("foo")) == None)

    // inconsistent data
    assert(Await.result(client2.add("client2-only", "bar")) == true)
    assert(Await.result(client1.get("client2-only")) == None)
    assert(Await.result(client2.get("client2-only")) == Some(stringToChannelBuffer("bar")))
    assert(Await.result(replicatedClient.delete("client2-only")) == false)

    // inconsistent replica state
    Await.result(client2.set("client2-only", "bar"))
    assert(Await.result(client1.get("client2-only")) == None)
    assert(Await.result(client2.get("client2-only")) == Some(stringToChannelBuffer("bar")))
    firstTestServerPool(0).stop()
    firstTestServerPool(1).stop()
    intercept[SimpleReplicationFailure] {
      Await.result(replicatedClient.delete("client2-only"))
    }
    secondTestServerPool(0).stop()
    secondTestServerPool(1).stop()
    intercept[SimpleReplicationFailure] {
      Await.result(replicatedClient.delete("client2-only"))
    }
  }

  test("simple replication client add & replace") {
    // create my cluster client solely based on a zk client and a path
    val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
    Await.result(mycluster1.ready) // give it sometime for the cluster to get the initial set of memberships
    val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
    Await.result(mycluster2.ready) // give it sometime for the cluster to get the initial set of memberships

    val client1 = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .group(Group.fromCluster(mycluster1))
        .failureAccrualParams(Int.MaxValue, 0.seconds)
        .build()
    val client2 = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .group(Group.fromCluster(mycluster2))
        .failureAccrualParams(Int.MaxValue, 0.seconds)
        .build()
    val replicatedClient = new SimpleReplicationClient(Seq(client1, client2))

    // consistent
    assert(Await.result(replicatedClient.add("foo", "bar")) == true)
    assert(Await.result(replicatedClient.get("foo")) == Some(stringToChannelBuffer("bar")))
    assert(Await.result(client1.get("foo")) == Some(stringToChannelBuffer("bar")))
    assert(Await.result(client2.get("foo")) == Some(stringToChannelBuffer("bar")))

    assert(Await.result(replicatedClient.replace("foo", "baz")) == true)
    assert(Await.result(client1.get("foo")) == Some(stringToChannelBuffer("baz")))
    assert(Await.result(client2.get("foo")) == Some(stringToChannelBuffer("baz")))

    assert(Await.result(replicatedClient.add("foo", "bar")) == false)

    assert(Await.result(replicatedClient.replace("no-such-key", "test")) == false)

    // inconsistent data
    assert(Await.result(client1.add("client1-only", "test")) == true)
    assert(Await.result(client2.add("client2-only", "test")) == true)
    assert(Await.result(replicatedClient.add("client2-only", "test")) == false)
    assert(Await.result(replicatedClient.replace("client1-only", "test")) == false)

    // inconsistent replica state
    firstTestServerPool(0).stop()
    firstTestServerPool(1).stop()
    intercept[SimpleReplicationFailure] {
      Await.result(replicatedClient.add("client2-only", "test"))
    }
    intercept[SimpleReplicationFailure] {
      Await.result(replicatedClient.replace("client1-only", "test"))
    }
  }

  test("simple replication client incr & decr") {
    // create my cluster client solely based on a zk client and a path
    val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
    Await.result(mycluster1.ready) // give it sometime for the cluster to get the initial set of memberships
    val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
    Await.result(mycluster2.ready) // give it sometime for the cluster to get the initial set of memberships

    val client1 = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .group(Group.fromCluster(mycluster1))
        .failureAccrualParams(Int.MaxValue, 0.seconds)
        .build()
    val client2 = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .group(Group.fromCluster(mycluster2))
        .failureAccrualParams(Int.MaxValue, 0.seconds)
        .build()
    val replicatedClient = new SimpleReplicationClient(Seq(client1, client2))

    // consistent
    assert(Await.result(replicatedClient.incr("foo", 2L)) == None)
    assert(Await.result(replicatedClient.add("foo", "1")) == true)
    assert(Await.result(replicatedClient.get("foo")) == Some(stringToChannelBuffer("1")))
    assert(Await.result(replicatedClient.incr("foo", 2L)) == Some(3L))
    assert(Await.result(replicatedClient.decr("foo", 1L)) == Some(2L))

    // inconsistent data
    assert(Await.result(client2.incr("foo", 1L)) == Some(3L))
    assert(Await.result(replicatedClient.incr("foo", 2L)) == None)
    assert(Await.result(replicatedClient.decr("foo", 2L)) == None)

    // inconsistent replica state
    firstTestServerPool(0).stop()
    firstTestServerPool(1).stop()
    intercept[SimpleReplicationFailure] {
      Await.result(replicatedClient.incr("foo", 2L))
    }
    intercept[SimpleReplicationFailure] {
      Await.result(replicatedClient.decr("foo", 2L))
    }
  }

  test("simple replication client many keys") {
    // create my cluster client solely based on a zk client and a path
    val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
    Await.result(mycluster1.ready) // give it sometime for the cluster to get the initial set of memberships
    val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
    Await.result(mycluster2.ready) // give it sometime for the cluster to get the initial set of memberships

    val client1 = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .group(Group.fromCluster(mycluster1))
        .failureAccrualParams(Int.MaxValue, 0.seconds)
        .build()
    val client2 = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .group(Group.fromCluster(mycluster2))
        .failureAccrualParams(Int.MaxValue, 0.seconds)
        .build()
    val replicatedClient = new SimpleReplicationClient(Seq(client1, client2))

    val count = 100
    (0 until count).foreach{
      n => {
        Await.result(replicatedClient.set("foo"+n, "bar"+n))
      }
    }

    (0 until count).foreach {
      n => {
        assert(Await.result(replicatedClient.get("foo"+n)) == Some(stringToChannelBuffer("bar"+n)))
        assert(Await.result(client1.get("foo"+n)) == Some(stringToChannelBuffer("bar"+n)))
        assert(Await.result(client2.get("foo"+n)) == Some(stringToChannelBuffer("bar"+n)))
      }
    }

    // shutdown primary pool
    firstTestServerPool(0).stop()
    firstTestServerPool(1).stop()

    (0 until count).foreach {
      n => {
        assert(Await.result(replicatedClient.get("foo"+n)) == Some(stringToChannelBuffer("bar"+n)))
      }
    }
  }

  if (!Option(System.getProperty("SKIP_FLAKY")).isDefined) test("simple replication client replica down") {
    // create my cluster client solely based on a zk client and a path
    val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
    Await.result(mycluster1.ready) // give it sometime for the cluster to get the initial set of memberships
    val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
    Await.result(mycluster2.ready) // give it sometime for the cluster to get the initial set of memberships

    val client1 = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .group(Group.fromCluster(mycluster1))
        .failureAccrualParams(Int.MaxValue, 0.seconds)
        .build()
    val client2 = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .group(Group.fromCluster(mycluster2))
        .failureAccrualParams(Int.MaxValue, 0.seconds)
        .build()
    val replicatedClient = new SimpleReplicationClient(Seq(client1, client2))

    Await.result(replicatedClient.set("foo", "bar"))
    assert(Await.result(replicatedClient.get("foo")) == Some(stringToChannelBuffer("bar")))
    assert(Await.result(client1.get("foo")) == Some(stringToChannelBuffer("bar")))
    assert(Await.result(client2.get("foo")) == Some(stringToChannelBuffer("bar")))

    // primary pool down
    firstTestServerPool(0).stop()
    firstTestServerPool(1).stop()

    assert(Await.result(replicatedClient.get("foo")) == Some(stringToChannelBuffer("bar")))
    intercept[SimpleReplicationFailure] {
      Await.result(replicatedClient.set("foo", "baz"))
    }

    // bring back primary pool
    TestMemcachedServer.start(Some(firstTestServerPool(0).address))
    TestMemcachedServer.start(Some(firstTestServerPool(1).address))

    assert(Await.result(replicatedClient.get("foo")) == Some(stringToChannelBuffer("baz")))
    assert(Await.result(client1.get("foo")) == None)
    assert(Await.result(client2.get("foo")) == Some(stringToChannelBuffer("baz")))
    Await.result(replicatedClient.set("foo", "baz"))
  }

  test("simple replication client non supported operation") {
    // create my cluster client solely based on a zk client and a path
    val mycluster1 = CachePoolCluster.newZkCluster(firstPoolPath, zookeeperClient)
    Await.result(mycluster1.ready) // give it sometime for the cluster to get the initial set of memberships
    val mycluster2 = CachePoolCluster.newZkCluster(secondPoolPath, zookeeperClient)
    Await.result(mycluster2.ready) // give it sometime for the cluster to get the initial set of memberships

    val client1 = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .group(Group.fromCluster(mycluster1))
        .failureAccrualParams(Int.MaxValue, 0.seconds)
        .build()
    val client2 = KetamaClientBuilder()
        .clientBuilder(ClientBuilder().hostConnectionLimit(1).codec(Memcached()).failFast(false))
        .group(Group.fromCluster(mycluster2))
        .failureAccrualParams(Int.MaxValue, 0.seconds)
        .build()
    val replicatedClient = new SimpleReplicationClient(Seq(client1, client2))

    intercept[UnsupportedOperationException] {
      Await.result(replicatedClient.append("not-supported", "value"))
    }
    intercept[UnsupportedOperationException] {
      Await.result(replicatedClient.prepend("not-supported", "value"))
    }
  }
}
