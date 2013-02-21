package com.twitter.finagle.thriftmux

import com.twitter.finagle.Group
import com.twitter.finagle.ThriftMux
import com.twitter.util.Future
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

@RunWith(classOf[JUnitRunner])
class EndToEndTest extends FunSuite {
  test("end-to-end Scrooge") {
    import com.twitter.finagle.thriftmux.thriftscala.TestService
    val server = ThriftMux.serveIface(":*", new TestService.FutureIface {
      def query(x: String) = Future.value(x+x)
    })

    val client = ThriftMux.newIface[TestService.FutureIface](server)
    assert(client.query("ok").get() == "okok")
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
