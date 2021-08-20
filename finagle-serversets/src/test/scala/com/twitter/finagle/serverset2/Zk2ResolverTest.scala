package com.twitter.finagle.serverset2

import com.twitter.finagle.common.zookeeper.ServerSetImpl
import com.twitter.finagle.{Addr, Address, Name, Resolver}
import com.twitter.finagle.addr.WeightedAddress
import com.twitter.finagle.partitioning.zk.ZkMetadata
import com.twitter.finagle.zookeeper.ZkInstance
import com.twitter.util.RandomSocket
import java.net.InetSocketAddress
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration
import org.scalatest.time.{Span, SpanSugar}
import org.scalatest.BeforeAndAfter
import scala.jdk.CollectionConverters._
import org.scalatest.funsuite.AnyFunSuite

class Zk2ResolverTest extends AnyFunSuite with BeforeAndAfter with Eventually with SpanSugar {
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
  }

  after {
    inst.stop()
  }

  private[this] def zk2resolve(path: String): Name =
    Resolver.eval("zk2!" + inst.zookeeperConnectString + "!" + path)

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

  // This test isn't flaky so don't use the definition of test in this file
  super.test("statsOf takes the first two components of the first hostname") {
    assert(Zk2Resolver.statsOf("foo-bar.baz.twitter.com") == "foo-bar.baz")
    assert(Zk2Resolver.statsOf("foo-bar.baz.twitter.com,foo-bar2.baz.twitter.com") == "foo-bar.baz")
    assert(Zk2Resolver.statsOf("foo-bar,foo-baz") == "foo-bar")
    assert(
      Zk2Resolver.statsOf("some-very-very-very-long-hostname") == "some-very-very-very-long-hostn"
    )
    assert(Zk2Resolver.statsOf("localhost:2181") == "localhost:2181")
  }
}
