package com.twitter.finagle.mdns

import com.twitter.finagle.{Announcer, Resolver, Addr}
import com.twitter.util.{Await, Var}
import java.net.{InetSocketAddress, InetAddress, Socket}
import org.junit.runner.RunWith
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.concurrent.Timeouts._
import org.scalatest.junit.JUnitRunner
import org.scalatest.time.SpanSugar._
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class MdnsTest extends FunSuite with Eventually with IntegrationPatience {
  val loopback = InetAddress.getLoopbackAddress

  // CSL-1870: this isn't flaky. It doesn't work in CI because JmDNS uses port 5353 which is
  // not allowed on some CI hosts.
  if (!sys.props.contains("SKIP_FLAKY"))
  test("bind locally") {
    val rawIa = new InetSocketAddress(loopback, 0)

    // this is a terrible hack, but it's tricky to do better.
    // we want to use ephemeral ports in our tests, but we never need to
    // bind to a port in this test, so it's difficult to coordinate over resolution.
    // in order to get around this, we bind to a port explicitly, to convert our
    // ephemeral port to one we can pass over MDNS.  It's OK to race here, because
    // we can't collide, given that we never stand up a server to the port.
    val ia = {
      val socket = new Socket()
      try {
        socket.bind(rawIa)
        socket.getLocalSocketAddress().asInstanceOf[InetSocketAddress]
      } finally {
        socket.close()
      }
    }
    val resolver = new MDNSResolver
    val announcer = new MDNSAnnouncer
    val dest = "my-service._finagle._tcp.local."

    val announcement = Await.result(announcer.announce(ia, dest))
    try {
      val addr = resolver.bind(dest)

      eventually(timeout(5 seconds)) {
        Var.sample(addr) match {
          case Addr.Bound(sockaddrs, _) =>
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
    val sock = new InetSocketAddress(loopback, 0)
    Await.result(Announcer.announce(sock, "local!foo"))
    Await.result(Announcer.announce(sock, "mdns!foo._bar._tcp.local."))
  }

  test("throws an exception on an imporperly formatted name") {
    val res = new MDNSResolver
    val ann = new MDNSAnnouncer
    val ia = new InetSocketAddress(loopback, 0)
    intercept[MDNSAddressException] { ann.announce(ia, "invalidname") }
    intercept[MDNSAddressException] { res.bind("invalidname") }
  }

  test("name parser") {
    val (name, regType, domain) = MDNS.parse("dots.in.the.name._finagle._tcp.local.")
    assert(name == "dots.in.the.name")
    assert(regType == "_finagle._tcp")
    assert(domain == "local")
  }
}
