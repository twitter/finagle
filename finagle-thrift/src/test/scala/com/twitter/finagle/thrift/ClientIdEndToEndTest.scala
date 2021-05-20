package com.twitter.finagle.thrift

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Service
import com.twitter.finagle.service.ResponseClassifier
import com.twitter.test._
import com.twitter.util.{Await, Future}
import java.util.{List => JList}
import org.apache.thrift.protocol.TProtocolFactory
import scala.reflect.ClassTag
import org.scalatest.funsuite.AnyFunSuite

class ClientIdEndToEndTest extends AnyFunSuite with ThriftTest {
  type Iface = B.ServiceIface
  def ifaceManifest = implicitly[ClassTag[B.ServiceIface]]

  val processor = new B.ServiceIface {
    def add(a: Int, b: Int): Future[Integer] = Future.exception(new AnException)
    def add_one(a: Int, b: Int): Future[Void] = Future.Void
    def multiply(a: Int, b: Int): Future[Integer] = Future { a * b }
    // Re-purpose `complex_return` to return the serversize ClientId.
    def complex_return(someString: String): Future[SomeStruct] = Future {
      val clientIdStr = ClientId.current.map(_.name).getOrElse("")
      new SomeStruct(123, clientIdStr)
    }
    def someway(): Future[Void] = Future.Void
    def show_me_your_dtab(): Future[String] = Future.value("")
    def show_me_your_dtab_size(): Future[Integer] = Future.value(0)

    def mergeable_add(alist: JList[Integer]): Future[Integer] = Future.value(0)
  }

  val ifaceToService = new B.Service(_: Iface, _: RichServerParam)
  val serviceToIface = new B.ServiceToClient(
    _: Service[ThriftClientRequest, Array[Byte]],
    _: TProtocolFactory,
    ResponseClassifier.Default
  )

  val clientId = "test.devel"

  testThrift("end-to-end ClientId propagation", Some(ClientId(clientId))) { (client, _) =>
    // arg_two repurposed to be the serverside ClientId.
    val result = Await.result(client.complex_return(""), 10.seconds).arg_two
    assert(result == clientId)
  }

  testThrift("end-to-end empty ClientId propagation", None) { (client, _) =>
    // arg_two repurposed to be the serverside ClientId.
    val result = Await.result(client.complex_return(""), 10.seconds).arg_two
    assert(result == "")
  }

  runThriftTests()
}
