package com.twitter.finagle.serverset2

import collection.JavaConverters._
import com.twitter.common.zookeeper.ServerSetImpl
import com.twitter.conversions.time._
import com.twitter.finagle.zookeeper.ZkInstance
import com.twitter.finagle.{Addr, Resolver}
import com.twitter.thrift.Status._
import com.twitter.util.Duration
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicReference
import org.junit.runner.RunWith
import org.scalatest.concurrent.Eventually._
import org.scalatest.concurrent.Timeouts._
import org.scalatest.junit.JUnitRunner
import org.scalatest.time._
import org.scalatest.{FunSuite, BeforeAndAfter, Tag}

@RunWith(classOf[JUnitRunner])
class ZkResolverTest extends FunSuite with BeforeAndAfter {
  val zkTimeout = 100.milliseconds
  @volatile var inst: ZkInstance = _

  def toSpan(d: Duration): Span = Span(d.inNanoseconds, Nanoseconds)

  implicit val patienceConfig = PatienceConfig(
    timeout = toSpan(1.second),
    interval = toSpan(zkTimeout))

  before {
    inst = new ZkInstance
    inst.start()
  }

  after {
    inst.stop()
  }

  override def test(testName: String, testTags: Tag*)(f: => Unit) {
    if (!sys.props.contains("SKIP_FLAKY"))
      super.test(testName, testTags:_*)(f)
  }

  test("end-to-end: service endpoint") {
    val serverSet = new ServerSetImpl(inst.zookeeperClient, "/foo/bar")
    
    val ref = new AtomicReference[Addr]
    val v = Resolver.eval("zk2!"+inst.zookeeperConnectstring+"!/foo/bar").bind()
    val o = v.observeTo(ref)

    eventually { assert(ref.get === Addr.Neg) }

    val joinAddr = new InetSocketAddress(8080)
    val status = serverSet.join(joinAddr, Map[String, InetSocketAddress]().asJava, ALIVE)
    eventually { assert(ref.get === Addr.Bound(joinAddr)) }
    
    status.leave()
    eventually { assert(ref.get === Addr.Neg) }
      
    o.close()
  }
  
  test("end-to-end: additional endpoints") {
    val serverSet = new ServerSetImpl(inst.zookeeperClient, "/foo/bar")
    
    val refService, refEpep = new AtomicReference[Addr]
    val o1 = Resolver
      .eval("zk2!"+inst.zookeeperConnectstring+"!/foo/bar")
      .bind()
      .observeTo(refService)
    val o2 = Resolver
      .eval("zk2!"+inst.zookeeperConnectstring+"!/foo/bar!epep")
      .bind()
      .observeTo(refEpep)

    eventually {
      assert(refService.get === Addr.Neg)
      assert(refEpep.get === Addr.Neg)
    }

    val serviceAddr = new InetSocketAddress(8080)
    val epepAddr = new InetSocketAddress(8989)

    val status = serverSet.join(
      serviceAddr, 
      Map[String, InetSocketAddress]("epep" -> epepAddr).asJava, 
      ALIVE)
    eventually {
      assert(refService.get === Addr.Bound(serviceAddr))
      assert(refEpep.get === Addr.Bound(epepAddr))
    }
    
    status.leave()
    eventually {
      assert(refService.get === Addr.Neg)
      assert(refEpep.get === Addr.Neg)
    }
     
    o1.close(); o2.close()
  }
}