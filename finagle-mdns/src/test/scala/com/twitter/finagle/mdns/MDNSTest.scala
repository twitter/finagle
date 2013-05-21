package com.twitter.finagle.mdns

import com.twitter.finagle.{Announcer, Resolver}
import com.twitter.util.{Await, RandomSocket}
import java.net.InetSocketAddress
import org.junit.runner.RunWith
import org.scalatest.concurrent.Eventually._
import org.scalatest.concurrent.Timeouts._
import org.scalatest.junit.JUnitRunner
import org.scalatest.time.SpanSugar._
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class MdnsTest extends FunSuite with BeforeAndAfter {
  test("bind locally") {
    val addr = RandomSocket()
    val resolver = new MDNSResolver
    val announcer = new MDNSAnnouncer
    val target = "my-service._finagle._tcp.local."

    val announcement = Await.result(announcer.announce(addr, target))
    try {
      val group = resolver.resolve(target).get() map { _.asInstanceOf[InetSocketAddress].getPort }
      eventually(timeout(5 seconds)) { assert(group().contains(addr.getPort)) }
    } finally {
      Await.ready(announcement.close())
    }
  }

  test("resolve via the main resolver") {
    assert(Resolver.resolve("mdns!foo._bar._tcp.local.").isReturn)
    assert(Resolver.resolve("local!foo").isReturn)
  }

  test("announce via the main announcer") {
    val sock = RandomSocket()
    assert(Await.ready(Announcer.announce(sock, "local!foo")).isReturn)
    assert(Await.ready(Announcer.announce(sock, "mdns!foo._bar._tcp.local.")).isReturn)
  }

  test("throws an exception on an imporperly formatted name") {
    val res = new MDNSResolver
    val ann = new MDNSAnnouncer
    val addr = new InetSocketAddress(0)
    intercept[MDNSTargetException] { ann.announce(addr, "invalidname") }
    intercept[MDNSTargetException] { res.resolve("invalidname") }
  }
}
