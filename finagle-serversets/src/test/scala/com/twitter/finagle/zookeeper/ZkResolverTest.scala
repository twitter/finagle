package com.twitter.finagle.zookeeper

import com.twitter.common.zookeeper.ServerSetImpl
import com.twitter.conversions.time._
import com.twitter.finagle.Resolver
import com.twitter.thrift.Status._
import com.twitter.util.Await
import com.twitter.util.Duration
import java.net.InetSocketAddress
import org.junit.runner.RunWith
import org.scalatest.concurrent.Eventually._
import org.scalatest.concurrent.Timeouts._
import org.scalatest.junit.JUnitRunner
import org.scalatest.time._
import org.scalatest.{BeforeAndAfter, FunSuite}
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class ZkResolverTest extends FunSuite with BeforeAndAfter {
  val zkTimeout = 100.milliseconds
  var inst: ZkInstance = _
  val factory = new ZkClientFactory(zkTimeout)

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

  def toSpan(d: Duration): Span = Span(d.inNanoseconds, Nanoseconds)

  test("represent the underlying ServerSet") {
    val serverSet = new ServerSetImpl(inst.zookeeperClient, "/foo/bar/baz")
    val clust = new ZkGroup(serverSet, "/foo/bar/baz")
    assert(clust().isEmpty)

    val status8080 = serverSet.join(
      new InetSocketAddress(8080),
      Map[String, InetSocketAddress]().asJava, ALIVE)

    eventually { assert(clust().size == 1) }
    val ep = clust().head.getServiceEndpoint
    assert(ep.getHost == "0.0.0.0")
    assert(ep.getPort == 8080)

    assert(clust() === clust())
    val snap = clust()

    val status8081 = serverSet.join(
      new InetSocketAddress(8081),
      Map[String, InetSocketAddress]().asJava, ALIVE)

    eventually { assert(clust().size == 2) }
    assert {
      val Seq(fst) = (clust() &~ snap).toSeq
      fst.getServiceEndpoint.getPort == 8081
    }
  }

if (!Option(System.getProperty("SKIP_FLAKY")).isDefined)
  test("resolve ALIVE endpoints") {
    val res = new ZkResolver(factory)
    val clust = res.resolve("localhost:%d!/foo/bar/baz".format(inst.zookeeperAddress.getPort))()
    assert(clust().isEmpty)
    val inetClust = clust collect { case ia: InetSocketAddress => ia }
    assert(inetClust() === inetClust())
    val serverSet = new ServerSetImpl(inst.zookeeperClient, "/foo/bar/baz")
    val addr = new InetSocketAddress("127.0.0.1", 8080)
    val blahAddr = new InetSocketAddress("10.0.0.1", 80)
    val status8080 = serverSet.join(
      addr,
      Map[String, InetSocketAddress]("blah" -> blahAddr).asJava, ALIVE)
    eventually { assert(inetClust().size == 1) }
    assert {
      val Seq(addr1) = inetClust().toSeq
      addr == addr1
    }
    status8080.leave()
    eventually { assert(inetClust().isEmpty) }
    serverSet.join(
      addr,
      Map[String, InetSocketAddress]("blah" -> blahAddr).asJava, ALIVE)
    eventually { assert(inetClust().size == 1) }

    val blahClust = res.resolve("localhost:%d!/foo/bar/baz!blah".format(inst.zookeeperAddress.getPort))()
    eventually { assert(blahClust().size == 1) }
    assert(blahClust() === blahClust())
    assert {
      val Seq(addr1) = blahClust().toSeq
      addr1 == blahAddr
    }
  }

  test("resolves from the main resolver") {
    assert(Resolver.resolve("zk!localhost:%d!/foo/bar/baz!blah".format(inst.zookeeperAddress.getPort)).isReturn)
  }
}
