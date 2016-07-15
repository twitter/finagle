package com.twitter.finagle.server

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.util.registry.{GlobalRegistry, SimpleRegistry, Entry}
import com.twitter.util.{Await, Future, Promise}
import java.net.{SocketAddress, Socket, InetSocketAddress, InetAddress}
import org.junit.runner.RunWith
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import scala.util.control.NonFatal

@RunWith(classOf[JUnitRunner])
class StringServerTest extends FunSuite
  with StringServer
  with Eventually
  with IntegrationPatience {

  test("StringServer notices when the client cuts the connection") {
    val p = Promise[String]()
    @volatile var interrupted = false
    p.setInterruptHandler { case NonFatal(t) =>
      interrupted = true
    }
    @volatile var observedRequest: Option[String] = None

    val service = new Service[String, String] {
      def apply(request: String) = {
        observedRequest = Some(request)
        p
      }
    }

    val server = stringServer.serve(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
      service)

    val client = new Socket()
    eventually { client.connect(server.boundAddress) }

    client.getOutputStream.write("hello netty4!\n".getBytes("UTF-8"))
    client.getOutputStream.flush()
    eventually { assert(observedRequest == Some("hello netty4!")) }

    client.close()
    eventually { assert(interrupted) }

    Await.ready(server.close(), 2.seconds)
  }

  test("exports listener type to registry") {
    val registry = new SimpleRegistry
    val label = "stringServer"

    val listeningServer = GlobalRegistry.withRegistry(registry) {
      stringServer.withLabel(label)
        .serve(":*", Service.mk[String, String](Future.value(_)))
    }

    val expectedEntry = Entry(
      key = Seq("server", StringServer.protocolLibrary, label, "Listener"),
      value = "Netty3Listener")

    assert(registry.iterator.contains(expectedEntry))

    Await.result(listeningServer.close(), 5.seconds)
  }
}
