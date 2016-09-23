package com.twitter.finagle.thrift

import com.twitter.finagle.{Address, Name, Thrift}
import com.twitter.finagle.thrift.thriftscala.{Echo, ExtendedEcho}
import com.twitter.util.{Await, Future}
import java.net.{InetAddress, InetSocketAddress}
import org.junit.runner.RunWith
import org.scalatest.{Finders, FunSuite}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MultiplexedServiceTest extends FunSuite {

  test("serve multiple ifaces") {

    class EchoService extends Echo.FutureIface {
      override def echo(msg: String): Future[String] = Future.value(msg)
    }

    class ExtendedEchoService extends ExtendedEcho.FutureIface {
      override def echo(msg: String): Future[String] = Future.value(msg)
      override def getStatus(): Future[String] = Future.value("OK")
    }

    val serviceMap = Map(
      "echo" -> new EchoService(),
      "extendedEcho" -> new ExtendedEchoService()
    )

    val address = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val server = Thrift.server.serveIfaces(address, serviceMap, Some("extendedEcho"))

    val name = Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress]))
    val client = Thrift.client.multiplex(name, "client") { client =>
      new {
        val echo = client.newIface[Echo.FutureIface]("echo")
        val extendedEcho = client.newServiceIface[ExtendedEcho.ServiceIface]("extendedEcho")
      }
    }

    assert(Await.result(client.echo.echo("hello")) == "hello")
    assert(Await.result(client.extendedEcho.getStatus(ExtendedEcho.GetStatus.Args())) == ExtendedEcho.GetStatus.Result(Some("OK")))

    val classicClient = Thrift.client.newIface[ExtendedEcho.FutureIface](name, "classic-client")
    assert(Await.result(classicClient.getStatus()) == "OK")
  }
}
