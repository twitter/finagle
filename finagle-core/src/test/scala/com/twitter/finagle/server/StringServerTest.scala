package com.twitter.finagle.server

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.client.StringClient
import com.twitter.util.registry.{Entry, GlobalRegistry, SimpleRegistry}
import com.twitter.util.{Await, Future, Promise}
import java.net.{InetAddress, InetSocketAddress, Socket}
import org.junit.runner.RunWith
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import scala.util.control.NonFatal

@RunWith(classOf[JUnitRunner])
class StringServerTest extends FunSuite
  with StringServer
  with StringClient
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

  trait Ctx {
    val svc = new Service[String, String] {
      def apply(request: String): Future[String] = {
        Future.value(request)
      }
    }

    val address = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val registry = ServerRegistry.connectionRegistry(address)

    val server = stringServer.serve(address, svc)
    val boundAddress = server.boundAddress.asInstanceOf[InetSocketAddress]

    val client1 = stringClient.newService(Name.bound(Address(boundAddress)), "stringClient1")
    val client2 = stringClient.newService(Name.bound(Address(boundAddress)), "stringClient2")
  }

  test("ConnectionRegistry has the right size") {
    new Ctx {
      val initialRegistrySize = registry.iterator.size

      assert(Await.result(client1("hello"), 1.second) == "hello")
      eventually {
        assert((registry.iterator.size - initialRegistrySize) == 1)
      }

      assert(Await.result(client2("foo"), 1.second) == "foo")
      eventually {
        assert((registry.iterator.size - initialRegistrySize) == 2)
      }

      Await.result(client1.close(), 5.seconds)
      eventually {
        assert((registry.iterator.size - initialRegistrySize) == 1)
      }

      Await.result(server.close(), 5.seconds)
      Await.result(client2.close(), 5.seconds)
      eventually {
        assert((registry.iterator.size - initialRegistrySize) == 0)
      }
    }
  }

  test("ConnectionRegistry correctly removes entries upon client close") {
    new Ctx {
      val initialState = registry.iterator.toArray

      assert(Await.result(client1("hello"), 1.second) == "hello")
      val remoteAddr1 = registry
        .iterator
        .find(!initialState.contains(_))
        .get

      assert(Await.result(client2("foo"), 1.second) == "foo")
      val remoteAddr2 = registry
        .iterator
        .find { a => !initialState.contains(a) && a != remoteAddr1}
        .get

      Await.result(client2.close(), 5.seconds)
      eventually {
        val addresses = registry.iterator.toArray
        assert(addresses.contains(remoteAddr1))
        assert(!(addresses.contains(remoteAddr2)))
      }

      Await.result(server.close(), 5.seconds)
      Await.result(client1.close(), 5.seconds)
    }
  }
}
