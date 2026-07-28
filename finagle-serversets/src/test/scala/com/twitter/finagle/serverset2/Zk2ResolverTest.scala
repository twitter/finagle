package com.twitter.finagle.serverset2

import com.twitter.finagle.common.zookeeper.ServerSetImpl
import com.twitter.finagle.Addr
import com.twitter.finagle.Address
import com.twitter.finagle.InetResolver
import com.twitter.finagle.Name
import com.twitter.finagle.Resolver
import com.twitter.finagle.addr.WeightedAddress
import com.twitter.finagle.partitioning.zk.ZkMetadata
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.zookeeper.ZkInstance
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.RandomSocket
import com.twitter.util.Var
import com.twitter.util.Witness
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.UnknownHostException
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.ZooDefs.Ids
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar
import org.scalatest.BeforeAndAfter
import scala.jdk.CollectionConverters._
import org.scalatest.funsuite.AnyFunSuite

object Zk2ResolverTest {

  private val HostCount = 10

  private val Path = "/foo/bar"

  // When resolving a 10-host serverset, causes the first resolution to return 1 address and the
  // second to return 10.
  private class FlakyDns(hostCount: Int) extends (String => Future[Seq[InetAddress]]) {
    private[this] val counter = new AtomicInteger(0)

    def lookups: Int = counter.get

    def apply(host: String): Future[Seq[InetAddress]] = {
      val n = counter.incrementAndGet()
      if (n > 1 && n <= hostCount) Future.exception(new UnknownHostException(host))
      else Future.value(Seq(InetAddress.getLoopbackAddress))
    }
  }
}

class Zk2ResolverTest extends AnyFunSuite with BeforeAndAfter with Eventually with SpanSugar {
  import Zk2ResolverTest._

  val zkTimeout: Span = 100.milliseconds

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = 45.seconds, interval = zkTimeout)

  // The Zk2 resolver has a hardcoded session timeout of 10 seconds and a stabilization epoch of
  // 40 seconds. We give these tests double that to observe nodes leaving a serverset.
  // Because this is so high, we don't check more than once every 5 seconds.
  @volatile var inst: ZkInstance = _
  val stabilizationEpoch: Span = 40.seconds
  val stabilizationTimeout: PatienceConfiguration.Timeout =
    PatienceConfiguration.Timeout(stabilizationEpoch * 2)
  val stabilizationInterval: PatienceConfiguration.Interval =
    PatienceConfiguration.Interval(5.seconds)

  val shardId = 42
  val emptyMetadata = Map.empty[String, String]

  before {
    inst = new ZkInstance
    inst.start()
    // Increase session timeout from 100ms default to avoid disconnects mid-test
    inst.zookeeperServer.setMinSessionTimeout(4000)
    inst.zookeeperServer.setMaxSessionTimeout(40000)
  }

  after {
    inst.stop()
  }

  private[this] def zk2resolve(path: String, hosts: Option[String] = None): Name =
    Resolver.eval("zk2!" + hosts.getOrElse(inst.zookeeperConnectString) + "!" + path)

  private[this] def address(
    ia: InetSocketAddress,
    shardIdOpt: Option[Int] = Some(shardId),
    metadata: Map[String, String] = emptyMetadata
  ): Address =
    WeightedAddress(
      Address.Inet(ia, ZkMetadata.toAddrMetadata(ZkMetadata(shardIdOpt, metadata))),
      1.0)

  test("end-to-end: service endpoint") {
    val Name.Bound(va) = zk2resolve("/foo/bar")
    eventually {
      assert(va.sample() == Addr.Neg, "resolution is not negative before serverset exists")
    }

    val serverSet = new ServerSetImpl(inst.zookeeperClient, "/foo/bar")
    val joinAddr = RandomSocket()
    val status = serverSet.join(joinAddr, Map.empty[String, InetSocketAddress].asJava, shardId)
    eventually {
      assert(
        va.sample() == Addr.Bound(address(joinAddr)),
        "resolution is not bound once the serverset exists"
      )
    }

    status.leave()
    eventually(stabilizationTimeout, stabilizationInterval) {
      assert(va.sample() == Addr.Neg, "resolution is not negative after the serverset disappears")
    }
  }

  test("end-to-end: additional endpoints") {
    val Name.Bound(va1) = zk2resolve("/foo/bar")
    val Name.Bound(va2) = zk2resolve("/foo/bar!epep")
    eventually {
      assert(va1.sample() == Addr.Neg, "resolution is not negative before serverset exists")
      assert(va2.sample() == Addr.Neg, "resolution is not negative before serverset exists")
    }

    val serverSet = new ServerSetImpl(inst.zookeeperClient, "/foo/bar")
    val serviceAddr = RandomSocket()
    val epepAddr = RandomSocket()
    val status = serverSet.join(serviceAddr, Map("epep" -> epepAddr).asJava, shardId)
    eventually {
      assert(
        va1.sample() == Addr.Bound(address(serviceAddr)),
        "resolution is not bound once the serverset exists"
      )
      assert(
        va2.sample() == Addr.Bound(address(epepAddr)),
        "resolution is not bound once the serverset exists"
      )
    }

    status.leave()
    eventually(stabilizationTimeout, stabilizationInterval) {
      assert(va1.sample() == Addr.Neg, "resolution is not negative after the serverset disappears")
      assert(va2.sample() == Addr.Neg, "resolution is not negative after the serverset disappears")
    }
  }

  test("end-to-end: no shard ID") {
    val Name.Bound(va) = zk2resolve("/foo/bar")
    eventually {
      assert(va.sample() == Addr.Neg, "resolution is not negative before serverset exists")
    }

    val serverSet = new ServerSetImpl(inst.zookeeperClient, "/foo/bar")
    val joinAddr = RandomSocket()
    val status = serverSet.join(joinAddr, Map.empty[String, InetSocketAddress].asJava)
    eventually {
      assert(
        va.sample() == Addr.Bound(address(joinAddr, None)),
        "resolution is not bound once the serverset exists"
      )
    }

    status.leave()
    eventually(stabilizationTimeout, stabilizationInterval) {
      assert(va.sample() == Addr.Neg, "resolution is not negative after the serverset disappears")
    }
  }

  test("end-to-end: service endpoint with metadata") {
    val Name.Bound(va) = zk2resolve("/foo/bar")
    eventually {
      assert(va.sample() == Addr.Neg, "resolution is not negative before serverset exists")
    }

    val metadataMap = Map("someKey" -> "someValue")
    val metadataJMap = metadataMap.asJava
    val serverSet = new ServerSetImpl(inst.zookeeperClient, "/foo/bar")
    val joinAddr = RandomSocket()
    val status =
      serverSet.join(joinAddr, Map.empty[String, InetSocketAddress].asJava, shardId, metadataJMap)
    eventually {
      assert(
        va.sample() == Addr.Bound(address(joinAddr, Some(shardId), metadataMap)),
        "resolution is not bound once the serverset exists"
      )
    }

    status.leave()
    eventually(stabilizationTimeout, stabilizationInterval) {
      assert(va.sample() == Addr.Neg, "resolution is not negative after the serverset disappears")
    }
  }

  test("end-to-end: multiple ZK clusters") {
    // Create a second ZK cluster
    val inst2 = new ZkInstance
    inst2.start()

    val zkHosts = s"${inst.zookeeperConnectString}#${inst2.zookeeperConnectString}"
    val Name.Bound(va) = zk2resolve("/foo/bar", Some(zkHosts))
    eventually {
      assert(va.sample() == Addr.Neg, "resolution is not negative before serverset exists")
    }

    val serverSet1 = new ServerSetImpl(inst.zookeeperClient, "/foo/bar")
    val serverSet2 = new ServerSetImpl(inst2.zookeeperClient, "/foo/bar")

    val addr1 = RandomSocket()
    val addr2 = RandomSocket()

    val status1 =
      serverSet1.join(addr1, Map.empty[String, InetSocketAddress].asJava, shardId)
    val status2 =
      serverSet2.join(addr2, Map.empty[String, InetSocketAddress].asJava, shardId + 1)

    eventually {
      assert(
        va.sample() == Addr.Bound(address(addr1, Some(shardId)), address(addr2, Some(shardId + 1))),
        "resolution is not bound once the serverset exists"
      )
    }

    status2.leave()
    eventually {
      assert(
        va.sample() == Addr.Bound(address(addr1, Some(shardId))),
        "resolution is not bound once the serverset exists"
      )
    }

    status1.leave()
    eventually(stabilizationTimeout, stabilizationInterval) {
      assert(va.sample() == Addr.Neg, "resolution is not negative after the serverset disappears")
    }

    inst2.stop()
  }

  test("end-to-end: -1 weighted endpoints are filtered") {
    val Name.Bound(va) = zk2resolve("/foo/bar")
    eventually {
      assert(va.sample() == Addr.Neg, "resolution is not negative before serverset exists")
    }

    val serviceAddr = RandomSocket()
    val filteredAddr = RandomSocket()
    val serverSet = new ServerSetImpl(inst.zookeeperClient, "/foo/bar")
    serverSet.join(serviceAddr, Map("https" -> RandomSocket()).asJava, shardId)
    serverSet.join(filteredAddr, Map("https" -> RandomSocket()).asJava, shardId + 1)

    eventually {
      assert(
        va.sample() == Addr.Bound(address(serviceAddr), address(filteredAddr, Some(shardId + 1))),
        "resolution is not bound once the serverset exists"
      )
    }

    // create vector node to set the weight of filteredAddr to -1
    val vector = s"""{"vector": [{"select": "shard=${shardId + 1}", "weight": -1.0}]}"""
    val zkClient = inst.zookeeperClient.get()
    zkClient.create(
      "/foo/bar/vector_0000000000",
      vector.getBytes(),
      Ids.OPEN_ACL_UNSAFE,
      CreateMode.EPHEMERAL
    )

    eventually {
      assert(
        va.sample() == Addr.Bound(address(serviceAddr)),
        "address with a weight of -1 is not filtered from va"
      )
    }

    // delete vector node to remove the weight of -1 from filteredAddr
    zkClient.delete("/foo/bar/vector_0000000000", 0)

    eventually {
      assert(
        va.sample() == Addr.Bound(address(serviceAddr), address(filteredAddr, Some(shardId + 1))),
        "filteredAddr should re-join va"
      )
    }
  }

  // These test aren't flaky so don't use the definition of test in this file
  super.test("statsOf takes the first two components of the first hostname") {
    assert(Zk2Resolver.statsOf("foo-bar.baz.twitter.com") == "foo-bar.baz")
    assert(Zk2Resolver.statsOf("foo-bar.baz.twitter.com,foo-bar2.baz.twitter.com") == "foo-bar.baz")
    assert(Zk2Resolver.statsOf("foo-bar,foo-baz") == "foo-bar")
    assert(
      Zk2Resolver.statsOf("some-very-very-very-long-hostname") == "some-very-very-very-long-hostn"
    )
    assert(Zk2Resolver.statsOf("localhost:2181") == "localhost:2181")
  }

  super.test("merge: ignore non-bound updates if there is at least one Addr.Bound") {
    val va1 = Var(Addr.Pending: Addr)
    val va2 = Var(Addr.Pending: Addr)
    val mergedVa = Zk2Resolver.merge(Seq(va1, va2))

    assert(mergedVa.sample() == Addr.Pending)

    val bound = Addr.Bound(address(RandomSocket()))

    va1() = bound
    assert(mergedVa.sample() == bound)

    va2() = Addr.Neg
    assert(mergedVa.sample() == bound)

    va2() = Addr.Failed(new Exception("boom"))
    assert(mergedVa.sample() == bound)
  }

  super.test("merge: ensure pending > neg > failed") {
    val va1 = Var(Addr.Pending: Addr)
    val va2 = Var(Addr.Pending: Addr)
    val va3 = Var(Addr.Pending: Addr)
    val mergedVa = Zk2Resolver.merge(Seq(va1, va2, va3))

    assert(mergedVa.sample() == Addr.Pending)

    va3() = Addr.Neg
    assert(mergedVa.sample() == Addr.Pending)

    va2() = Addr.Failed(new Exception("boom"))
    assert(mergedVa.sample() == Addr.Pending)

    // only neg and failed, should be neg
    va1() = Addr.Neg
    assert(mergedVa.sample() == Addr.Neg)

    va3() = Addr.Failed(new Exception("clap"))
    assert(mergedVa.sample() == Addr.Neg)

    // all failed
    va1() = Addr.Failed(new Exception("charlixcx"))
    assert(
      mergedVa.sample() match {
        case Addr.Failed(_) => true
        case _ => false
      }
    )
  }

  test("addrOf shares serverset resolution for stats / clients") {
    def join(serverSet: ServerSetImpl, shardId: Int): Unit = {
      val member = new InetSocketAddress(InetAddress.getLoopbackAddress, 10000 + shardId)
      serverSet.join(member, Map.empty[String, InetSocketAddress].asJava, shardId)
    }

    val zkScope = Zk2Resolver.statsOf(inst.zookeeperConnectString)

    val serverSet = new ServerSetImpl(inst.zookeeperClient, Path)
    (1 to HostCount).foreach(join(serverSet, _))

    val dns = new FlakyDns(HostCount)
    val stats = new InMemoryStatsReceiver
    val resolver = new Zk2Resolver(
      statsReceiver = stats,
      stabilizerWindow = Duration.fromMilliseconds(100),
      unhealthyWindow = Duration.fromMinutes(5),
      // No pollInterval; this is what we use in production (since updates arrive via ZK
      // watches).
      inetResolver = new InetResolver(dns, NullStatsReceiver, pollIntervalOpt = None),
      timer = DefaultTimer
    )

    val published = new AtomicReference[Addr]
    val observation = resolver
      .addrOf(inst.zookeeperConnectString, Path, None, None)
      .changes
      .register(Witness(published))

    try {
      eventually {
        assert(Zk2Resolver.sizeOf(published.get) > 0)
      }

      assert(dns.lookups == HostCount)

      assert(Zk2Resolver.sizeOf(published.get) == 1)
      assert(stats.gauges(Seq(zkScope, "foo", "bar", "endpoint=default", "size"))() == 1f)

      // limbo is nstable - nunstable
      // (1 - 1) = 0
      assert(stats.gauges(Seq(zkScope, "foo", "bar", "endpoint=default", "limbo"))() == 0f)
      assert(
        stats.gauges(
          Seq(zkScope, "foo", "bar", "endpoint=default", "members"))() == HostCount.toFloat)

      // Many stabilizer epochs later nothing has moved. An epoch only promotes the
      // buffer, the buffer only moves when the raw address emits, and with no poll
      // interval the address never re-resolves on its own.
      Thread.sleep(Duration.fromSeconds(2).inMilliseconds)
      assert(dns.lookups == HostCount)
      assert(Zk2Resolver.sizeOf(published.get) == 1)
      assert(stats.gauges(Seq(zkScope, "foo", "bar", "endpoint=default", "size"))() == 1f)
      assert(stats.gauges(Seq(zkScope, "foo", "bar", "endpoint=default", "limbo"))() == 0f)
      assert(
        stats.gauges(
          Seq(zkScope, "foo", "bar", "endpoint=default", "members"))() == HostCount.toFloat)

      // A change to the serverset is the only thing that triggers re-resolution
      join(serverSet, HostCount + 1)
      eventually {
        assert(Zk2Resolver.sizeOf(published.get) == HostCount + 1)
        assert(
          stats.gauges(
            Seq(zkScope, "foo", "bar", "endpoint=default", "size"))() == (HostCount + 1).toFloat)
        assert(stats.gauges(Seq(zkScope, "foo", "bar", "endpoint=default", "limbo"))() == 0f)
        assert(
          stats.gauges(Seq(zkScope, "foo", "bar", "endpoint=default", "members"))()
            == (HostCount + 1).toFloat)
      }

      assert(dns.lookups == HostCount + HostCount + 1)
    } finally observation.close()
  }
}
