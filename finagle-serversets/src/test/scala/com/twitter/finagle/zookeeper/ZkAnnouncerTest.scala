package com.twitter.finagle.zookeeper

import com.twitter.finagle.addr.WeightedAddress
import com.twitter.finagle.partitioning.zk.ZkMetadata
import com.twitter.finagle.{Addr, Address, Announcer, Name, Resolver}
import com.twitter.util.{Await, Duration, Var}
import java.io.{BufferedReader, InputStreamReader}
import java.net.{InetSocketAddress, URL}
import org.scalatest.concurrent.Eventually
import org.scalatest.exceptions.TestFailedDueToTimeoutException
import org.scalatest.time.{Span, SpanSugar}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class ZkAnnouncerTest extends AnyFunSuite with BeforeAndAfter with Eventually with SpanSugar {

  val zkTimeout: Span = 100.milliseconds
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = 45.seconds, interval = zkTimeout)

  @volatile var inst: ZkInstance = _
  val port1 = 80
  val port2 = 81
  val shardId = 0
  val path = "/foo/bar/baz"
  val emptyMetadata = Map.empty[String, String]
  val factory = new ZkClientFactory(toDuration(zkTimeout))

  before {
    inst = new ZkInstance
    inst.start()
  }

  after {
    inst.stop()
  }

  def toDuration(s: Span): Duration = Duration.fromNanoseconds(s.totalNanos)

  private[this] def zk2resolve(path: String): Name =
    Resolver.eval("zk2!" + inst.zookeeperConnectString + "!" + path)

  private[this] def zk2resolve(path: String, endpoint: String): Name =
    Resolver.eval("zk2!" + inst.zookeeperConnectString + "!" + path + "!" + endpoint)

  def hostPath: String = "localhost:%d!%s".format(inst.zookeeperAddress.getPort, path)

  private[this] def zk2ResolvedAddress(
    ia: InetSocketAddress,
    shardIdOpt: Option[Int] = Some(shardId),
    metadata: Map[String, String] = emptyMetadata
  ): Address =
    WeightedAddress(
      Address.Inet(ia, ZkMetadata.toAddrMetadata(ZkMetadata(shardIdOpt, metadata))),
      1.0)

  test("announce a primary endpoint") {
    val ann = new ZkAnnouncer(factory)
    val res = new ZkResolver(factory)
    val addr = Address.Inet(new InetSocketAddress(port1), Addr.Metadata.empty)
    Await.result(ann.announce(addr.addr, "%s!0".format(hostPath)))

    val va = res.bind(hostPath)
    eventually {
      Var.sample(va) match {
        case Addr.Bound(sockaddrs, attrs) if attrs.isEmpty =>
          assert(sockaddrs == Set(addr))
        case _ => fail()
      }
    }
  }

  test("announce a primary endpoint with metadata") {
    val ann = new ZkAnnouncer(factory)
    val metadata = Map("keyA" -> "valueA")
    val addrInet = new InetSocketAddress(port1)
    val addr = Address.Inet(addrInet, Addr.Metadata.empty)
    Await.result(ann.announce(addr.addr, "%s!%d".format(hostPath, shardId), metadata))

    val Name.Bound(va) = zk2resolve(path)
    eventually {
      assert(
        va.sample() == Addr.Bound(zk2ResolvedAddress(addrInet, Some(shardId), metadata))
      )
    }
  }

  test("announce a primary endpoint with metadata and additional endpoints") {
    val ann = new ZkAnnouncer(factory)
    val metadata = Map("keyA" -> "valueA")
    val addrInet = new InetSocketAddress(port1)
    val addr = Address.Inet(addrInet, Addr.Metadata.empty)
    val additionalEndpointInet = new InetSocketAddress(port2)
    val additionalEndpointAddr = Address.Inet(additionalEndpointInet, Addr.Metadata.empty)
    Await.result(
      ann.announce(
        addr.addr,
        "%s!%d".format(hostPath, shardId),
        metadata,
        Map("endpoint" -> additionalEndpointAddr.addr)))

    val Name.Bound(va) = zk2resolve(path, "endpoint")
    eventually {
      assert(
        va.sample() ==
          Addr.Bound(zk2ResolvedAddress(additionalEndpointInet, Some(shardId), metadata))
      )
    }
  }

  test("only announce additional endpoints if a primary endpoint is present") {
    var va1: Var[Addr] = null
    var va2: Var[Addr] = null
    var failedEventually = 1

    try {
      val ann = new ZkAnnouncer(factory)
      val res = new ZkResolver(factory)
      val addr1 = Address.Inet(new InetSocketAddress(port1), Addr.Metadata.empty)
      val addr2 = Address.Inet(new InetSocketAddress(port2), Addr.Metadata.empty)

      Await.ready(ann.announce(addr2.addr, "%s!0!addr2".format(hostPath)))
      va2 = res.bind("%s!addr2".format(hostPath))
      eventually { assert(Var.sample(va2) != Addr.Pending) }
      failedEventually += 1
      assert(Var.sample(va2) == Addr.Neg)

      Await.ready(ann.announce(addr1.addr, "%s!0".format(hostPath)))
      va1 = res.bind(hostPath)
      eventually { assert(Var.sample(va2) == Addr.Bound(addr2)) }
      failedEventually += 1
      eventually { assert(Var.sample(va1) == Addr.Bound(addr1)) }
    } catch {
      case e: TestFailedDueToTimeoutException =>
        var exceptionString = "#%d eventually failed.\n".format(failedEventually)

        if (va1 != null) {
          exceptionString += "va1 status: %s\n".format(Var.sample(va1).toString)
        }

        if (va2 != null) {
          exceptionString += "va2 status: %s\n".format(Var.sample(va2).toString)
        }

        val endpoint = "/services/ci"
        val connection = new URL("http", "0.0.0.0", 4680, endpoint).openConnection()
        val reader = new BufferedReader(new InputStreamReader(connection.getInputStream))
        var fullOutput = ""
        var line = reader.readLine()
        while (line != null) {
          fullOutput += line
          line = reader.readLine()
        }
        exceptionString += "output from %s -- %s".format(endpoint, fullOutput)

        throw new Exception(exceptionString)
    }
  }

  test("unannounce additional endpoints, but not primary endpoints") {
    val ann = new ZkAnnouncer(factory)
    val res = new ZkResolver(factory)
    val addr1 = Address.Inet(new InetSocketAddress(port1), Addr.Metadata.empty)
    val addr2 = Address.Inet(new InetSocketAddress(port2), Addr.Metadata.empty)

    val anm1 = Await.result(ann.announce(addr1.addr, "%s!0".format(hostPath)))
    val anm2 = Await.result(ann.announce(addr2.addr, "%s!0!addr2".format(hostPath)))
    val va1 = res.bind(hostPath)
    val va2 = res.bind("%s!addr2".format(hostPath))

    eventually { assert(Var.sample(va1) == Addr.Bound(addr1)) }
    eventually { assert(Var.sample(va2) == Addr.Bound(addr2)) }

    Await.result(anm2.unannounce())

    eventually { assert(Var.sample(va2) == Addr.Neg) }
    assert(Var.sample(va1) == Addr.Bound(addr1))
  }

  test("unannounce primary endpoints and additional endpoints") {
    val ann = new ZkAnnouncer(factory)
    val res = new ZkResolver(factory)
    val addr1 = Address.Inet(new InetSocketAddress(port1), Addr.Metadata.empty)
    val addr2 = Address.Inet(new InetSocketAddress(port2), Addr.Metadata.empty)

    val anm1 = Await.result(ann.announce(addr1.addr, "%s!0".format(hostPath)))
    val anm2 = Await.result(ann.announce(addr2.addr, "%s!0!addr2".format(hostPath)))
    val va1 = res.bind(hostPath)
    val va2 = res.bind("%s!addr2".format(hostPath))

    eventually { assert(Var.sample(va1) == Addr.Bound(addr1)) }
    eventually { assert(Var.sample(va2) == Addr.Bound(addr2)) }

    Await.ready(anm1.unannounce())

    eventually { assert(Var.sample(va1) == Addr.Neg) }
    eventually { assert(Var.sample(va2) == Addr.Neg) }
  }

  test("announces from the main announcer") {
    val addr = Address.Inet(new InetSocketAddress(port1), Addr.Metadata.empty)
    Await.result(Announcer.announce(addr.addr, "zk!%s!0".format(hostPath)))
  }

}
