package com.twitter.finagle.thrift

import com.twitter.finagle.Service
import com.twitter.finagle.Thrift.param.ServiceClass
import com.twitter.finagle.thrift.thriftscala.{Echo => ScalaEcho}
import com.twitter.finagle.thrift.thriftjava.{Echo => JavaEcho}
import com.twitter.finagle.thrift.thriftscala.Echo.Echo
import com.twitter.scrooge.{Request, Response}
import com.twitter.util.Future
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

  class EchoFutureIface extends ScalaEcho.FutureIface {
    override def echo(msg: String): Future[String] = ???
  }

  class EchoServiceIface extends JavaEcho.ServiceIface {
    override def echo(msg: String): Future[String] = ???
  }
}

class ServiceClassParamTest extends AnyFunSuite {
  import ServiceClassParamTest._

  private def fqn[A: ClassTag]: String = ServiceClass(
    Some(classTag[A].runtimeClass)).fullyQualifiedName.get

  test("extractServiceFqn for Scala clients") {
    assert(fqn[ScalaEcho.MethodPerEndpoint] == "com.twitter.finagle.thrift.thriftscala.Echo")
    assert(fqn[ScalaEcho.ServicePerEndpoint] == "com.twitter.finagle.thrift.thriftscala.Echo")
    assert(fqn[ScalaEcho.ServiceIface] == "com.twitter.finagle.thrift.thriftscala.Echo")
  }

  test("extractServiceFqn for Java clients") {
    assert(fqn[JavaEcho.ServiceIface] == "com.twitter.finagle.thrift.thriftjava.Echo")
  }

  test("extractServiceFqn for scala servers") {
    assert(fqn[EchoMPE] == "com.twitter.finagle.thrift.thriftscala.Echo")
    assert(fqn[EchoSPE] == "com.twitter.finagle.thrift.thriftscala.Echo")
    assert(fqn[EchoReqRepSPE] == "com.twitter.finagle.thrift.thriftscala.Echo")
    assert(fqn[EchoFutureIface] == "com.twitter.finagle.thrift.thriftscala.Echo")
  }

  test("extractServiceFqn for java servers") {
    assert(fqn[EchoServiceIface] == "com.twitter.finagle.thrift.thriftjava.Echo")
  }
}
