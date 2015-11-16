package com.twitter.finagle.thrift

import com.twitter.finagle.Service
import com.twitter.test._
import com.twitter.util.{Await, Future}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import scala.reflect.ClassTag

@RunWith(classOf[JUnitRunner])
class ClientIdEndToEndTest extends FunSuite with ThriftTest {
  type Iface = B.ServiceIface
  def ifaceManifest = implicitly[ClassTag[B.ServiceIface]]

  val processor = new B.ServiceIface {
    def add(a: Int, b: Int) = Future.exception(new AnException)
    def add_one(a: Int, b: Int) = Future.Void
    def multiply(a: Int, b: Int) = Future { a * b }
    // Re-purpose `complex_return` to return the serversize ClientId.
    def complex_return(someString: String) = Future {
      val clientIdStr = ClientId.current map { _.name } getOrElse("")
      new SomeStruct(123, clientIdStr)
    }
    def someway() = Future.Void
    def show_me_your_dtab() = Future.value("")
    def show_me_your_dtab_size() = Future.value(0)
  }

  val ifaceToService = new B.Service(_, _)
  val serviceToIface = new B.ServiceToClient(_, _)

  val clientId = "test.devel"

  testThrift("end-to-end ClientId propagation", Some(ClientId(clientId))) { (client, _) =>
    // arg_two repurposed to be the serverside ClientId.
    val result = Await.result(client.complex_return("")).arg_two
    assert(result == clientId)
  }

  testThrift("end-to-end empty ClientId propagation", None) { (client, _) =>
    // arg_two repurposed to be the serverside ClientId.
    val result = Await.result(client.complex_return("")).arg_two
    assert(result == "")
  }

  runThriftTests()
}
