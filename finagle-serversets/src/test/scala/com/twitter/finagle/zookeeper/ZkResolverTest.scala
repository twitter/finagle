package com.twitter.finagle.zookeeper

import com.twitter.common.zookeeper.ServerSetImpl
import com.twitter.conversions.time._
import com.twitter.finagle.{Addr, Resolver}
import com.twitter.thrift.Status._
import com.twitter.util.{Await, Duration, RandomSocket, Var}
import java.net.InetSocketAddress
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.concurrent.Eventually._
import org.scalatest.concurrent.Timeouts._
import org.scalatest.junit.JUnitRunner
import org.scalatest.time._
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

  // Flaky tests. See https://jira.twitter.biz/browse/COORD-437 for details.
  if (!sys.props.contains("SKIP_FLAKY")) {
    test("represent the underlying ServerSet") {
      val serverSet = new ServerSetImpl(inst.zookeeperClient, "/foo/bar/baz")
      val clust = new ZkGroup(serverSet, "/foo/bar/baz")
      assert(clust().isEmpty)

      val ephAddr1 = RandomSocket.nextAddress
      val ephAddr2 = RandomSocket.nextAddress

      serverSet.join(ephAddr1, Map[String, InetSocketAddress]().asJava, ALIVE)

      eventually { assert(clust().size == 1) }
      val ep = clust().head.getServiceEndpoint
      assert(ep.getHost == "0.0.0.0")
      assert(ep.getPort == ephAddr1.getPort)

      assert(clust() == clust())
      val snap = clust()

      serverSet.join(ephAddr2, Map[String, InetSocketAddress]().asJava, ALIVE)

      eventually { assert(clust().size == 2) }
      assert {
        val Seq(fst) = (clust() &~ snap).toSeq
        fst.getServiceEndpoint.getPort == ephAddr2.getPort
      }
    }

    test("filter by shardid") {
      val path = "/bar/foo/baz"
      val serverSet = new ServerSetImpl(inst.zookeeperClient, path)
      val clust = new ZkGroup(serverSet, path)
      assert(clust().isEmpty)

      // assert that 3 hosts show up in an unfiltered cluster
      val ephAddr1 = RandomSocket.nextAddress
      val ephAddr2 = RandomSocket.nextAddress
      val ephAddr3 = RandomSocket.nextAddress

      Seq(ephAddr1, ephAddr2, ephAddr3).foreach { sockAddr =>
        serverSet.join(
          sockAddr,
          Map[String, InetSocketAddress]().asJava,
          sockAddr.getPort
        ).update(ALIVE)
      }

      eventually { assert(clust().size == 3) }

      // and 1 in a cluster filtered by shardid (== to the port in this case)
      val filteredAddr =
        new ZkResolver(factory).resolve(
          Set(inst.zookeeperAddress),
          path,
          shardId = Some(ephAddr2.getPort)
        )
      eventually {
        Var.sample(filteredAddr) match {
          case Addr.Bound(addrs, attrs) if addrs.size == 1 && attrs.isEmpty => true
          case _ => fail()
        }
      }
    }

    test("resolve ALIVE endpoints") {
      val res = new ZkResolver(factory)
      val va = res.bind("localhost:%d!/foo/bar/baz".format(
        inst.zookeeperAddress.getPort))
      eventually { Var.sample(va) == Addr.Bound() }

      /*
       val inetClust = clust collect { case ia: InetSocketAddress => ia }
       assert(inetClust() == inetClust())
       */

      val serverSet = new ServerSetImpl(inst.zookeeperClient, "/foo/bar/baz")
      val port1 = RandomSocket.nextPort()
      val port2 = RandomSocket.nextPort()
      val sockAddr = new InetSocketAddress("127.0.0.1", port1)
      val blahAddr = new InetSocketAddress("10.0.0.1", port2)

      val status = serverSet.join(
        sockAddr,
        Map[String, InetSocketAddress]("blah" -> blahAddr).asJava,
        ALIVE
      )

      eventually { assert(Var.sample(va) == Addr.Bound(sockAddr)) }
      status.leave()
      eventually { assert(Var.sample(va) == Addr.Neg) }
      serverSet.join(
        sockAddr,
        Map[String, InetSocketAddress]("blah" -> blahAddr).asJava, ALIVE)
      eventually { assert(Var.sample(va) == Addr.Bound(sockAddr)) }

      val blahVa = res.bind("localhost:%d!/foo/bar/baz!blah".format(
        inst.zookeeperAddress.getPort))
      eventually { assert(Var.sample(blahVa) == Addr.Bound(blahAddr)) }
    }

    test("filter by endpoint") {
      val path = "/bar/foo/baz"
      val serverSet = new ServerSetImpl(inst.zookeeperClient, path)
      val clust = new ZkGroup(serverSet, path)
      assert(clust().isEmpty)

      // assert that 3 hosts show up in an unfiltered cluster
      val ephAddr1 = RandomSocket.nextAddress
      val ephAddr2 = RandomSocket.nextAddress
      val ephAddr3 = RandomSocket.nextAddress
      Seq(ephAddr1, ephAddr2, ephAddr3).foreach { sockAddr =>
        serverSet.join(
          sockAddr,
          Map[String, InetSocketAddress](sockAddr.getPort.toString -> sockAddr).asJava
        ).update(ALIVE)
      }

      eventually { assert(clust().size == 3) }

      val filteredAddr =
        new ZkResolver(factory).resolve(
          Set(inst.zookeeperAddress),
          path,
          endpoint = Some(ephAddr1.getPort.toString)
        )

      eventually {
        Var.sample(filteredAddr) match {
          case Addr.Bound(addrs, attrs) if addrs.size == 1 && attrs.isEmpty => true
          case _ => fail()
        }
      }
    }

    test("resolves from the main resolver") {
      Resolver.eval("zk!localhost:%d!/foo/bar/baz!blah".format(
        inst.zookeeperAddress.getPort))
    }
  }
}
