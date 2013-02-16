package com.twitter.finagle

import com.twitter.util.{Closable, Future, Time}
import java.net.{InetSocketAddress, SocketAddress}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

case class TestAnnouncement(addr: InetSocketAddress, target: String) extends Announcement {
  def unannounce() = Future.Done
}

class TestAnnouncer extends Announcer {
  val scheme = "test"
  def announce(addr: InetSocketAddress, target: String) =
    Future.value(TestAnnouncement(addr, target))
}

@RunWith(classOf[JUnitRunner])
class AnnouncerTest extends FunSuite {
  val addr = new InetSocketAddress(0)

  test("reject bad names") {
    assert(Announcer.announce(addr, "!foo!bar").isThrow)
  }

  test("reject unknown announcers") {
    assert(Announcer.announce(addr, "unkown!foobar").isThrow)
  }

  test("resolve ServiceLoaded announcers") {
    Announcer.announce(addr, "test!xyz").get() match {
      case p: TestAnnouncement => assert(p === TestAnnouncement(addr, "xyz"))
      case _ => assert(false)
    }
  }
}
