package com.twitter.finagle.zookeeper

import com.twitter.finagle.common.zookeeper.ServerSet
import com.twitter.finagle.common.zookeeper.ServerSetImpl
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Addr
import com.twitter.finagle.Address
import com.twitter.finagle.Resolver
import com.twitter.finagle.common.net.pool.DynamicHostSet
import com.twitter.thrift.ServiceInstance
import com.twitter.thrift.Status._
import com.twitter.util.Duration
import com.twitter.util.RandomSocket
import com.twitter.util.Var
import java.net.InetSocketAddress
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually
import org.scalatest.time._
import scala.jdk.CollectionConverters._
import org.scalatest.funsuite.AnyFunSuite

class ZkResolverTest extends AnyFunSuite with BeforeAndAfter with Eventually {
  private class ZkGroup(serverSet: ServerSet, path: String)
      extends Thread("ZkGroup(%s)".format(path)) {
    setDaemon(true)
    start()

    protected[finagle] val set = Var(Set[ServiceInstance]())

    def apply(): Set[ServiceInstance] = set()

    override def run(): Unit = {
      serverSet.watch(new DynamicHostSet.HostChangeMonitor[ServiceInstance] {
        def onChange(newSet: java.util.Set[ServiceInstance]): Unit = synchronized {
          set() = newSet.asScala.toSet
        }
      })
    }
  }

  val zkTimeout: Duration = 100.milliseconds
  var inst: ZkInstance = _
  val factory = new ZkClientFactory(zkTimeout)

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = toSpan(1.second), interval = toSpan(zkTimeout))

  before {
    inst = new ZkInstance
    inst.start()
  }

  after {
    inst.stop()
  }

  def toSpan(d: Duration): Span = Span(d.inNanoseconds, Nanoseconds)

  // Flaky tests. See COORD-437 for details.
  if (!sys.props.contains("SKIP_FLAKY")) {
    test("represent the underlying ServerSet") {
      val serverSet = new ServerSetImpl(inst.zookeeperClient, "/foo/bar/baz")
      val clust = new ZkGroup(serverSet, "/foo/bar/baz")
      assert(clust().isEmpty)

      val ephAddr1 = RandomSocket.nextAddress()
      val ephAddr2 = RandomSocket.nextAddress()

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
      val ephAddr1 = RandomSocket.nextAddress()
      val ephAddr2 = RandomSocket.nextAddress()
      val ephAddr3 = RandomSocket.nextAddress()

      Seq(ephAddr1, ephAddr2, ephAddr3).foreach { sockAddr =>
        serverSet
          .join(
            sockAddr,
            Map[String, InetSocketAddress]().asJava,
            sockAddr.getPort
          )
          .update(ALIVE)
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
      val va = res.bind("localhost:%d!/foo/bar/baz".format(inst.zookeeperAddress.getPort))
      eventually { Var.sample(va) == Addr.Bound() }

      /*
       val inetClust = clust collect { case ia: InetSocketAddress => ia }
       assert(inetClust() == inetClust())
       */

      val serverSet = new ServerSetImpl(inst.zookeeperClient, "/foo/bar/baz")
      val port1 = RandomSocket.nextPort()
      val port2 = RandomSocket.nextPort()
      val sockAddr = Address.Inet(new InetSocketAddress("127.0.0.1", port1), Addr.Metadata.empty)
      val blahAddr = Address.Inet(new InetSocketAddress("10.0.0.1", port2), Addr.Metadata.empty)

      val status = serverSet.join(
        sockAddr.addr,
        Map[String, InetSocketAddress]("blah" -> blahAddr.addr).asJava,
        ALIVE
      )

      eventually { assert(Var.sample(va) == Addr.Bound(sockAddr)) }
      status.leave()
      eventually { assert(Var.sample(va) == Addr.Neg) }
      serverSet.join(
        sockAddr.addr,
        Map[String, InetSocketAddress]("blah" -> blahAddr.addr).asJava,
        ALIVE
      )
      eventually { assert(Var.sample(va) == Addr.Bound(sockAddr)) }

      val blahVa = res.bind("localhost:%d!/foo/bar/baz!blah".format(inst.zookeeperAddress.getPort))
      eventually { assert(Var.sample(blahVa) == Addr.Bound(blahAddr)) }
    }

    test("filter by endpoint") {
      val path = "/bar/foo/baz"
      val serverSet = new ServerSetImpl(inst.zookeeperClient, path)
      val clust = new ZkGroup(serverSet, path)
      assert(clust().isEmpty)

      // assert that 3 hosts show up in an unfiltered cluster
      val ephAddr1 = RandomSocket.nextAddress()
      val ephAddr2 = RandomSocket.nextAddress()
      val ephAddr3 = RandomSocket.nextAddress()
      Seq(ephAddr1, ephAddr2, ephAddr3).foreach { sockAddr =>
        serverSet
          .join(
            sockAddr,
            Map[String, InetSocketAddress](sockAddr.getPort.toString -> sockAddr).asJava
          )
          .update(ALIVE)
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
      Resolver.eval("zk!localhost:%d!/foo/bar/baz!blah".format(inst.zookeeperAddress.getPort))
    }
  }
}
