package com.twitter.finagle.thrift

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.thrift.thriftscala.{Echo, ExtendedEcho}
import com.twitter.finagle.{Address, Name, Thrift}
import com.twitter.util.{Await, Future}
import java.net.{InetAddress, InetSocketAddress}
import org.scalatest.funsuite.AnyFunSuite

class MultiplexedServiceTest extends AnyFunSuite {

  def await[A](f: Future[A]): A = Await.result(f, 5.seconds)

  test("serve multiple services") {
    import scala.language.reflectiveCalls

    class EchoService extends Echo.MethodPerEndpoint {
      override def echo(msg: String): Future[String] = Future.value(msg)
    }

    class ExtendedEchoService extends ExtendedEcho.MethodPerEndpoint {
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
        val echo = client.build[Echo.MethodPerEndpoint]("echo")
        val extendedEcho =
          client.servicePerEndpoint[ExtendedEcho.ServicePerEndpoint]("extendedEcho")
      }
    }

    assert(await(client.echo.echo("hello")) == "hello")

    val actualStatus = await(client.extendedEcho.getStatus(ExtendedEcho.GetStatus.Args()))
    assert(actualStatus == "OK")

    val classicClient = Thrift.client.build[ExtendedEcho.MethodPerEndpoint](name, "classic-client")
    assert(await(classicClient.getStatus()) == "OK")
  }

}
