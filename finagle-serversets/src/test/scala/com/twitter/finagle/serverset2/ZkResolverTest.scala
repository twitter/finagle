package com.twitter.finagle.serverset2

import collection.JavaConverters._
import com.twitter.common.zookeeper.ServerSetImpl
import com.twitter.conversions.time._
import com.twitter.finagle.{Addr, Resolver, Name, WeightedSocketAddress}
import com.twitter.finagle.zookeeper.ZkInstance
import com.twitter.util.{Duration, RandomSocket, Var}
import java.net.InetSocketAddress
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, BeforeAndAfter, Tag}
import org.scalatest.concurrent.Eventually._
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}
import org.scalatest.time._

@RunWith(classOf[JUnitRunner])
class ZkResolverTest extends FunSuite with BeforeAndAfter with AssertionsForJUnit {
  val zkTimeout = 100.milliseconds
  @volatile var inst: ZkInstance = _

  implicit def toSpan(d: Duration): Span = Span(d.inNanoseconds, Nanoseconds)

  implicit val patienceConfig = PatienceConfig(
    timeout = 5.seconds,
    interval = zkTimeout)

  // The Zk2 resolver has a hardcoded session timeout of 10 seconds and a stabilization epoch of
  // 40 seconds. We give these tests double that to observe nodes leaving a serverset.
  // Because this is so high, we don't check more than once every 5 seconds.
  val stabilizationEpoch = 40.seconds
  val stabilizationTimeout = Timeout(stabilizationEpoch * 2)
  val stabilizationInterval = Interval(5.seconds)

  before {
    inst = new ZkInstance
    inst.start()
  }

  after {
    inst.stop()
  }

  override def test(testName: String, testTags: Tag*)(f: => Unit) {
    // Since this test currently relies on timing, it's currently best to treat it as flaky for CI.
    // It should be runnable, if a little slow, however.
    if (!sys.props.contains("SKIP_FLAKY"))
      super.test(testName, testTags:_*)(f)
  }

  private[this] def zk2resolve(path: String): Name =
    Resolver.eval("zk2!"+inst.zookeeperConnectstring+"!"+path)

  test("end-to-end: service endpoint") {
    val Name.Bound(va) = zk2resolve("/foo/bar")
    eventually {
      assert(va.sample() === Addr.Neg,
        "resolution is not negative before serverset exists")
    }

    val serverSet = new ServerSetImpl(inst.zookeeperClient, "/foo/bar")
    val joinAddr = RandomSocket()
    val status = serverSet.join(joinAddr, Map.empty[String, InetSocketAddress].asJava)
    eventually {
      assert(va.sample() === Addr.Bound(WeightedSocketAddress(joinAddr, 1.0)),
        "resolution is not bound once the serverset exists")
    }

    status.leave()
    eventually(stabilizationTimeout, stabilizationInterval) {
      assert(va.sample() === Addr.Neg,
        "resolution is not negative after the serverset disappears")
    }
  }

  test("end-to-end: additional endpoints") {
    val Name.Bound(va1) = zk2resolve("/foo/bar")
    val Name.Bound(va2) = zk2resolve("/foo/bar!epep")
    eventually {
      assert(va1.sample() === Addr.Neg,
        "resolution is not negative before serverset exists")
      assert(va2.sample() === Addr.Neg,
        "resolution is not negative before serverset exists")
    }

    val serverSet = new ServerSetImpl(inst.zookeeperClient, "/foo/bar")
    val serviceAddr = RandomSocket()
    val epepAddr = RandomSocket()
    val status = serverSet.join(serviceAddr,  Map("epep" -> epepAddr).asJava)
    eventually {
      assert(va1.sample() === Addr.Bound(WeightedSocketAddress(serviceAddr, 1.0)),
        "resolution is not bound once the serverset exists")
      assert(va2.sample() === Addr.Bound(WeightedSocketAddress(epepAddr, 1.0)),
        "resolution is not bound once the serverset exists")
    }

    status.leave()
    eventually(stabilizationTimeout, stabilizationInterval) {
      assert(va1.sample() === Addr.Neg,
        "resolution is not negative after the serverset disappears")
      assert(va2.sample() === Addr.Neg,
        "resolution is not negative after the serverset disappears")
    }
  }
}
