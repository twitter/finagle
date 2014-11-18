package com.twitter.finagle

import com.twitter.util._
import java.net.InetSocketAddress
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}

@RunWith(classOf[JUnitRunner])
class ResolutionRaceTest extends FunSuite with AssertionsForJUnit {

  private[this] val Echoer = Service.mk[String, String](Future.value)

  /*
   * Tries to trigger a race condition related to inet resolution -- it has been observed that
   * the the load balancer may throw NoBrokersAvailableException if resolution is asynchronous.
   *
   * If this test fails intermittently, IT IS NOT FLAKY, it's broken.
   */
  // Fails in CI, see https://jira.twitter.biz/browse/CSL-1358
  if (!sys.props.contains("SKIP_FLAKY")) test("resolution raciness") {
    val server = Echo.serve(":*", Echoer)
    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val dest = "asyncinet!localhost:%d".format(addr.getPort)
    try {
      1 to 1000 foreach { i =>
        val phrase = "%03d [%s]".format(i, dest)
        val echo = Echo.newService(dest)
        val echoed = Await.result(echo(phrase))
        assert(echoed === phrase)
      }
    } catch {
      case _: NoBrokersAvailableException =>
        fail("resolution is racy")
    } finally {
      Await.result(server.close())
    }
  }
}
