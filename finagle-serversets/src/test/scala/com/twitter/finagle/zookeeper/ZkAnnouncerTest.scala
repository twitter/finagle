package com.twitter.finagle.zookeeper

import com.twitter.conversions.time._
import com.twitter.finagle.{Announcer, Resolver}
import com.twitter.util.Await
import com.twitter.util.Duration
import java.net.InetSocketAddress
import org.junit.runner.RunWith
import org.scalatest.concurrent.Eventually._
import org.scalatest.junit.JUnitRunner
import org.scalatest.time._
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class ZkAnnouncerTest extends FunSuite with BeforeAndAfter {
  val zkTimeout = 100.milliseconds
  var inst: ZkInstance = _
  val factory = new ZkClientFactory(zkTimeout)

  implicit val patienceConfig = PatienceConfig(
    timeout = toSpan(zkTimeout*3),
    interval = toSpan(zkTimeout))

  before {
    inst = new ZkInstance
    inst.start()
  }

  after {
    inst.stop()
  }

  def toSpan(d: Duration): Span = Span(d.inNanoseconds, Nanoseconds)
  def hostPath = "localhost:%d!/foo/bar/baz".format(inst.zookeeperAddress.getPort)

  test("announce a primary endpoint") {
    val ann = new ZkAnnouncer(factory)
    val res = new ZkResolver(factory)
    val addr = new InetSocketAddress(8080)
    Await.result(ann.announce(addr, "%s!0".format(hostPath)))

    val group = res.resolve(hostPath)() collect { case ia: InetSocketAddress => ia }

    eventually { assert(group().size == 1) }
    assert(Seq(addr) == group().toSeq)
  }

  test("only announce additional endpoints if a primary endpoint is present") {
    val ann = new ZkAnnouncer(factory)
    val res = new ZkResolver(factory)
    val addr1 = new InetSocketAddress(8080)
    val addr2 = new InetSocketAddress(8081)

    Await.ready(ann.announce(addr2, "%s!0!addr2".format(hostPath)))
    val addr2Group = res.resolve("%s!addr2".format(hostPath))() collect { case ia: InetSocketAddress => ia }

    assert(addr2Group().size == 0)

    Await.ready(ann.announce(addr1, "%s!0".format(hostPath)))
    val addr1Group = res.resolve(hostPath)() collect { case ia: InetSocketAddress => ia }

    eventually { assert(addr2Group().size == 1) }
    assert(Seq(addr2) == addr2Group().toSeq)

    eventually { assert(addr1Group().size == 1) }
    assert(Seq(addr1) == addr1Group().toSeq)
  }

  test("unannounce additional endpionts, but not primary endpoints") {
    val ann = new ZkAnnouncer(factory)
    val res = new ZkResolver(factory)
    val addr1 = new InetSocketAddress(8080)
    val addr2 = new InetSocketAddress(8081)

    val anm1 = Await.result(ann.announce(addr1, "%s!0".format(hostPath)))
    val anm2 = Await.result(ann.announce(addr2, "%s!0!addr2".format(hostPath)))
    val addr1Group = res.resolve(hostPath)() collect { case ia: InetSocketAddress => ia }
    val addr2Group = res.resolve("%s!addr2".format(hostPath))() collect { case ia: InetSocketAddress => ia }

    eventually { assert(addr1Group().size == 1) }
    eventually { assert(addr2Group().size == 1) }

    Await.ready(anm2.unannounce())

    eventually { assert(addr2Group().size == 0) }
    eventually { assert(addr1Group().size == 1) }
    assert(Seq(addr1) == addr1Group().toSeq)
  }

  test("unannounce primary endpoints and additional endpoints") {
    val ann = new ZkAnnouncer(factory)
    val res = new ZkResolver(factory)
    val addr1 = new InetSocketAddress(8080)
    val addr2 = new InetSocketAddress(8081)

    val anm1 = Await.result(ann.announce(addr1, "%s!0".format(hostPath)))
    val anm2 = Await.result(ann.announce(addr2, "%s!0!addr2".format(hostPath)))
    val addr1Group = res.resolve(hostPath)() collect { case ia: InetSocketAddress => ia }
    val addr2Group = res.resolve("%s!addr2".format(hostPath))() collect { case ia: InetSocketAddress => ia }

    eventually { assert(addr1Group().size == 1) }
    eventually { assert(addr2Group().size == 1) }

    Await.ready(anm1.unannounce())

    eventually { assert(addr2Group().size == 0) }
    eventually { assert(addr1Group().size == 0) }
  }

  test("announces from the main announcer") {
    val addr = new InetSocketAddress(8080)
    Await.result(Announcer.announce(addr, "zk!%s!0".format(hostPath)))
  }
}
