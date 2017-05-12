package com.twitter.finagle

import com.twitter.conversions.time._
import com.twitter.util._
import java.net.{InetAddress, InetSocketAddress}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ResolutionRaceTest extends FunSuite {

  private[this] val Echoer = Service.mk[String, String](Future.value)

  /*
   * Tries to trigger a race condition related to inet resolution -- it has been observed that
   * the the load balancer may throw NoBrokersAvailableException if resolution is asynchronous.
   *
   * If this test fails intermittently, IT IS NOT FLAKY, it's broken.
   * Or maybe its flakey in terms of port allocations.
   */
  test("resolution raciness") {
    val socketAddr = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val server = Echo.serve(socketAddr, Echoer)
    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val dest = s"asyncinet!localhost:${addr.getPort}"
    try {
      val phrase = s"[$dest]"
      val echo = Echo.newService(dest)
      try {
        val echoed = Await.result(echo(phrase), 5.seconds)
        assert(echoed == phrase)
      } finally Await.ready(echo.close(), 5.seconds)
    } finally {
      Await.result(server.close(), 5.seconds)
    }
  }
}
