package com.twitter.finagle.thrift

import com.twitter.finagle.Service
import com.twitter.finagle.Thrift.param.ServiceClass
import com.twitter.finagle.thrift.thriftscala.{Echo => ScalaEcho, ExtendedEcho => ScalaExtendedEcho}
import com.twitter.finagle.thrift.thriftjava.{Echo => JavaEcho, ExtendedEcho => JavaExtendedEcho}
import com.twitter.finagle.thrift.thriftscala.Echo.Echo
import com.twitter.scrooge.{Request, Response}
import com.twitter.util.Future
import java.util.NoSuchElementException
import org.scalatest.funsuite.AnyFunSuite
import scala.reflect.{ClassTag, classTag}

object ServiceClassParamTest {
  class EchoMPE extends ScalaEcho.MethodPerEndpoint {
    override def echo(msg: String): Future[String] = ???
  }

  class EchoSPE extends ScalaEcho.ServicePerEndpoint {
    override def echo: Service[Echo.Args, String] = ???
  }

  class EchoReqRepSPE extends ScalaEcho.ReqRepServicePerEndpoint {
    override def echo: Service[Request[Echo.Args], Response[String]] = ???
  }

  class EchoFutureIface extends ScalaEcho.MethodPerEndpoint {
    override def echo(msg: String): Future[String] = ???
  }

  class EchoServiceIface extends JavaEcho.ServiceIface {
    override def echo(msg: String): Future[String] = ???
  }

  class EchoIface extends JavaEcho.Iface {
    override def echo(msg: String): String = ???
  }

  class EchoService extends JavaEcho {}

  class EchoExtService extends JavaExtendedEcho {}

  class EchoFuture extends ScalaEcho.MethodPerEndpoint {
    override def echo(msg: String): Future[String] = ???
  }

  class EchoExtFuture extends ScalaExtendedEcho.MethodPerEndpoint {
    override def echo(msg: String): Future[String] = ???
    override def getStatus(): Future[String] = ???
  }
}

class ServiceClassParamTest extends AnyFunSuite {
  import ServiceClassParamTest._

  private def fqn[A: ClassTag]: String = ServiceClass(
    Some(classTag[A].runtimeClass)).fullyQualifiedName.get

  val expectedScala = "com.twitter.finagle.thrift.thriftscala.Echo"
  val expectedJava = "com.twitter.finagle.thrift.thriftjava.Echo"
  val expectedExtendedScala = "com.twitter.finagle.thrift.thriftscala.ExtendedEcho"
  val expectedExtendedJava = "com.twitter.finagle.thrift.thriftjava.ExtendedEcho"

  test("extractServiceFqn for scala clients") {
    assert(fqn[ScalaEcho.MethodPerEndpoint] == expectedScala)
    assert(fqn[ScalaEcho.ServicePerEndpoint] == expectedScala)
    assert(fqn[ScalaEcho.ServiceIface] == expectedScala)
    assert(fqn[ScalaEcho.MethodPerEndpoint] == expectedScala)
    assert(fqn[ScalaEcho.MethodPerEndpoint] == expectedScala)
    assert(fqn[ScalaExtendedEcho.MethodPerEndpoint] == expectedExtendedScala)
  }

  test("extractServiceFqn for java clients") {
    assert(fqn[JavaEcho.ServiceIface] == expectedJava)
    assert(fqn[JavaEcho.Iface] == expectedJava)
    assert(fqn[JavaEcho] == expectedJava)
    assert(fqn[JavaExtendedEcho] == expectedExtendedJava)
  }

  test("extractServiceFqn for scala servers") {
    assert(fqn[EchoMPE] == expectedScala)
    assert(fqn[EchoFutureIface] == expectedScala)
    assert(fqn[EchoFuture] == expectedScala)
    assert(fqn[EchoSPE] == expectedScala)
    assert(fqn[EchoReqRepSPE] == expectedScala)
    assert(fqn[EchoExtFuture] == expectedExtendedScala)
  }

  test("extractServiceFqn for java servers") {
    assert(fqn[EchoServiceIface] == expectedJava)
    assert(fqn[EchoIface] == expectedJava)
    assert(fqn[EchoService] == expectedJava)
    assert(fqn[EchoExtService] == expectedExtendedJava)
  }

  test("Invalid Finagle service class returns None") {
    class NotAService
    intercept[NoSuchElementException] {
      fqn[NotAService]
    }
  }
}
