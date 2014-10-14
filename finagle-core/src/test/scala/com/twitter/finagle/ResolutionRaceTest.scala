package com.twitter.finagle

import com.twitter.util._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}
import java.net.{SocketAddress, InetSocketAddress}
import scala.annotation.tailrec

@RunWith(classOf[JUnitRunner])
class ResolutionRaceTest extends FunSuite with AssertionsForJUnit {

  /**
   * Try to listen on random ports (no more than retries+1 times) until an available port is found.
   */
  @tailrec
  private[this] def listen(
    serveOn: SocketAddress => ListeningServer,
    retries: Int = 3
  ): (InetSocketAddress, ListeningServer) = {
    val socket = RandomSocket()
    Try(serveOn(socket)) match {
      case Return(server) => (socket, server)

      case Throw(_: org.jboss.netty.channel.ChannelException) if retries > 0 =>
        // port was probably in use, so try again.
        listen(serveOn, retries - 1)

      case Throw(e) =>
        throw e
    }
  }

  private[this] val Echoer = Service.mk[String, String](Future.value)

  /*
   * Tries to trigger a race condition related to inet resolution -- it has been observed that
   * the the load balancer may throw NoBrokersAvailableException if resolution is asynchronous.
   *
   * If this test fails intermittently, IT IS NOT FLAKY, it's broken.
   */
  test("resolution raciness") {
    val (socket, server) = listen(Echo.serve(_, Echoer))
    val dest = "asyncinet!localhost:%d".format(socket.getPort)
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
