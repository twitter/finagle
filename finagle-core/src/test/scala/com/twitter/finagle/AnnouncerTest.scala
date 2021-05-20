package com.twitter.finagle

import com.twitter.util.{Await, Future}
import java.net.{InetAddress, InetSocketAddress}
import org.scalatest.funsuite.AnyFunSuite

case class TestAnnouncement(ia: InetSocketAddress, addr: String) extends Announcement {
  def unannounce() = Future.Done
}

class TestAnnouncer extends Announcer {
  val scheme = "test"
  def announce(ia: InetSocketAddress, addr: String) =
    Future.value(TestAnnouncement(ia, addr))
}

class AnnouncerTest extends AnyFunSuite {
  val addr = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)

  test("reject bad names") {
    assert(Await.ready(Announcer.announce(addr, "!foo!bar")).poll.get.isThrow)
  }

  test("reject unknown announcers") {
    assert(Await.ready(Announcer.announce(addr, "unknown!foobar")).poll.get.isThrow)
  }

  test("resolve ServiceLoaded announcers") {
    Await.result(Announcer.announce(addr, "test!xyz")) match {
      case p: Proxy => assert(p.self == TestAnnouncement(addr, "xyz"))
      case _ => assert(false)
    }
  }

  test("provide a set of announcements") {
    Announcer.announce(addr, "test!xyz")
    assert(Announcer.announcements == Set((addr, List("test!xyz"))))
  }

  test("get an announcer instance") {
    val anmt = Await.result(Announcer.get(classOf[TestAnnouncer]).get.announce(addr, "foo"))
    assert(anmt == TestAnnouncement(addr, "foo"))
  }
}
