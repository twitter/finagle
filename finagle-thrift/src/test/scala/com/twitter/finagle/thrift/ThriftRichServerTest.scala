package com.twitter.finagle.thrift

import com.twitter.finagle.{ListeningServer, NullServer, ServiceFactory, Stack, Thrift}
import com.twitter.finagle.server.StackBasedServer
import com.twitter.finagle.thrift.thriftscala.Echo
import com.twitter.util.Future
import java.net.SocketAddress
import org.scalatest.funsuite.AnyFunSuite

object ThriftRichServerTest {
  class MockServer(val params: Stack.Params)(onServe: MockServer => Unit)
      extends StackBasedServer[Array[Byte], Array[Byte]]
      with ThriftRichServer {
    protected def serverParam: RichServerParam = RichServerParam()

    def withParams(ps: Stack.Params): StackBasedServer[Array[Byte], Array[Byte]] =
      new MockServer(ps)(onServe)

    def serve(
      addr: SocketAddress,
      service: ServiceFactory[Array[Byte], Array[Byte]]
    ): ListeningServer = {
      onServe(this)
      NullServer
    }

    def transformed(t: Stack.Transformer): StackBasedServer[Array[Byte], Array[Byte]] = this
  }

  class MockThriftService extends Echo.MethodPerEndpoint {
    def echo(msg: String): Future[String] = Future.value(msg)
  }
}

class ThriftRichServerTest extends AnyFunSuite {

  import ThriftRichServerTest._

  test("captures service class") {

    val service = new MockThriftService
    val server = new MockServer(Stack.Params.empty)(served =>
      assert(served.params[Thrift.param.ServiceClass].clazz == Some(classOf[MockThriftService])))

    server.serveIface(":*", service)
  }
}
