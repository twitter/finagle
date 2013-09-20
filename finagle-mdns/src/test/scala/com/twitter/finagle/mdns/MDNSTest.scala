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
    val ia = RandomSocket()
    val resolver = new MDNSResolver
    val announcer = new MDNSAnnouncer
    val addr = "my-service._finagle._tcp.local."

    val announcement = Await.result(announcer.announce(ia, addr))
    try {
      val group = resolver.resolve(addr).get() map { _.asInstanceOf[InetSocketAddress].getPort }
      eventually(timeout(5 seconds)) { assert(group().contains(ia.getPort)) }
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
    val ia = new InetSocketAddress(0)
    intercept[MDNSAddressException] { ann.announce(ia, "invalidname") }
    intercept[MDNSAddressException] { res.resolve("invalidname") }
  }
}
