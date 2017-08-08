package com.twitter.finagle.thrift

import com.twitter.conversions.time._
import com.twitter.finagle.thrift.thriftscala.{Echo, ExtendedEcho}
import com.twitter.finagle.{Address, Name, Thrift}
import com.twitter.util.{Await, Future}
import java.net.{InetAddress, InetSocketAddress}
import org.scalatest.FunSuite
import scala.language.reflectiveCalls

class MultiplexedServiceTest extends FunSuite {

  def await[A](f: Future[A]): A = Await.result(f, 5.seconds)

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

    assert(await(client.echo.echo("hello")) == "hello")

    val actualStatus = await(client.extendedEcho.getStatus(ExtendedEcho.GetStatus.Args()))
    assert(actualStatus == "OK")

    val classicClient = Thrift.client.newIface[ExtendedEcho.FutureIface](name, "classic-client")
    assert(await(classicClient.getStatus()) == "OK")
  }
}
