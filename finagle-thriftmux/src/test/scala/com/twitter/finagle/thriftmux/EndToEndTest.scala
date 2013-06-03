package com.twitter.finagle.thriftmux

import com.twitter.finagle.{Group, ThriftMux}
import com.twitter.util.{Await, Future}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class EndToEndTest extends FunSuite {
  test("end-to-end Scrooge2") {
    import com.twitter.finagle.thriftmux.thriftscrooge2.TestService
    val server = ThriftMux.serveIface(":*", new TestService.FutureIface {
      def query(x: String) = Future.value(x+x)
    })

    val client = ThriftMux.newIface[TestService.FutureIface](server)
    assert(client.query("ok").get() == "okok")
  }

  test("end-to-end Scrooge3") {
    import com.twitter.finagle.thriftmux.thriftscrooge3.TestService
    val server = ThriftMux.serveIface(":*", new TestService.FutureIface {
      def query(x: String) = Future.value(x+x)
    })

    val client = ThriftMux.newIface[TestService.FutureIface](server)
    assert(Await.result(client.query("ok")) == "okok")
  }
/* TODO: add back when sbt supports old-school thrift gen
  test("end-to-end finagle-thrift") {
    import com.twitter.finagle.thriftmux.thrift.TestService

    val server = ThriftMux.serveIface(":*", new TestService.ServiceIface {
      def query(x: String) = Future.value(x+x)
    })

    val client = ThriftMux.newIface[TestService.ServiceIface](server)
    assert(client.query("ok").get() == "okok")
  }
*/
}
