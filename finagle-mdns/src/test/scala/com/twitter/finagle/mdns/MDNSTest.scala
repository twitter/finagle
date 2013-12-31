package com.twitter.finagle.mdns

import com.twitter.finagle.{Announcer, Resolver, Addr}
import com.twitter.util.{Await, RandomSocket, Var}
import java.net.InetSocketAddress
import org.junit.runner.RunWith
import org.scalatest.concurrent.Eventually._
import org.scalatest.concurrent.Timeouts._
import org.scalatest.junit.JUnitRunner
import org.scalatest.time.SpanSugar._
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class MdnsTest extends FunSuite {
  if (!sys.props.contains("SKIP_FLAKY")) test("bind locally") {
    val ia = RandomSocket()
    val resolver = new MDNSResolver
    val announcer = new MDNSAnnouncer
    val dest = "my-service._finagle._tcp.local."

    val announcement = Await.result(announcer.announce(ia, dest))
    try {
      val addr = resolver.bind(dest)

      eventually(timeout(5 seconds)) {
        Var.sample(addr) match {
          case Addr.Bound(sockaddrs) =>
            assert(sockaddrs exists {
              case ia1: InetSocketAddress => ia1.getPort == ia.getPort
            })
          case _ => fail()
        }
      }
    } finally {
      Await.ready(announcement.close())
    }
  }

  test("resolve via the main resolver") {
    // (No exceptions)
    Resolver.eval("mdns!foo._bar._tcp.local.")
    Resolver.eval("local!foo")
  }

  test("announce via the main announcer") {
    val sock = RandomSocket()
    Await.result(Announcer.announce(sock, "local!foo"))
    Await.result(Announcer.announce(sock, "mdns!foo._bar._tcp.local."))
  }

  test("throws an exception on an imporperly formatted name") {
    val res = new MDNSResolver
    val ann = new MDNSAnnouncer
    val ia = new InetSocketAddress(0)
    intercept[MDNSAddressException] { ann.announce(ia, "invalidname") }
    intercept[MDNSAddressException] { res.bind("invalidname") }
  }

  test("name parser") {
    val (name, regType, domain) = MDNS.parse("dots.in.the.name._finagle._tcp.local.")
    assert(name === "dots.in.the.name")
    assert(regType === "_finagle._tcp")
    assert(domain === "local")
  }
}
