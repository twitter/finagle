package com.twitter.finagle.serverset2

import collection.JavaConverters._
import com.twitter.common.zookeeper.ServerSetImpl
import com.twitter.conversions.time._
import com.twitter.finagle.{Addr, Resolver, Name, WeightedSocketAddress}
import com.twitter.finagle.zookeeper.ZkInstance
import com.twitter.thrift.Status._
import com.twitter.util.{Duration, RandomSocket, Var}
import java.net.InetSocketAddress
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, BeforeAndAfter, Tag}
import org.scalatest.concurrent.Eventually._
import org.scalatest.concurrent.Timeouts._
import org.scalatest.junit.JUnitRunner
import org.scalatest.time._

@RunWith(classOf[JUnitRunner])
class ZkResolverTest extends FunSuite with BeforeAndAfter {
  val zkTimeout = 100.milliseconds
  @volatile var inst: ZkInstance = _

  def toSpan(d: Duration): Span = Span(d.inNanoseconds, Nanoseconds)

  implicit val patienceConfig = PatienceConfig(
    timeout = toSpan(5.seconds),
    interval = toSpan(zkTimeout))

  before {
    inst = new ZkInstance
    inst.start()
  }

  after {
    inst.stop()
  }

  override def test(testName: String, testTags: Tag*)(f: => Unit) {
    // COORD-329
    if (!sys.props.contains("SKIP_FLAKY"))
      super.test(testName, testTags:_*)(f)
  }

  test("end-to-end: service endpoint") {
    val serverSet = new ServerSetImpl(inst.zookeeperClient, "/foo/bar")
    val Name.Bound(va) = Resolver.eval("zk2!"+inst.zookeeperConnectstring+"!/foo/bar")

    eventually { assert(Var.sample(va) === Addr.Neg) }

    val joinAddr = RandomSocket.nextAddress
    val status = serverSet.join(joinAddr, Map[String, InetSocketAddress]().asJava, ALIVE)

    eventually { assert(Var.sample(va) === Addr.Bound(WeightedSocketAddress(joinAddr, 1.0))) }

    status.leave()

    eventually { assert(Var.sample(va) === Addr.Neg) }
  }

  test("end-to-end: additional endpoints") {
    val serverSet = new ServerSetImpl(inst.zookeeperClient, "/foo/bar")
    val Name.Bound(va1) = Resolver.eval("zk2!"+inst.zookeeperConnectstring+"!/foo/bar")
    val Name.Bound(va2) = Resolver.eval("zk2!"+inst.zookeeperConnectstring+"!/foo/bar!epep")

    eventually {
      assert(Var.sample(va1) === Addr.Neg)
      assert(Var.sample(va2) === Addr.Neg)
    }

    val serviceAddr = RandomSocket.nextAddress
    val epepAddr = RandomSocket.nextAddress

    val status = serverSet.join(
      serviceAddr, 
      Map[String, InetSocketAddress]("epep" -> epepAddr).asJava, 
      ALIVE
    )

    eventually {
      assert(Var.sample(va1) === Addr.Bound(serviceAddr))
      assert(Var.sample(va2) === Addr.Bound(epepAddr))
    }
    
    status.leave()

    eventually {
      assert(Var.sample(va1) === Addr.Neg)
      assert(Var.sample(va2) === Addr.Neg)
    }
  }
}
