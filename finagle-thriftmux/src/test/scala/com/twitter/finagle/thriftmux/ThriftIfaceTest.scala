package com.twitter.finagle.thriftmux

import com.twitter.finagle.ThriftMux
import com.twitter.util.Future
import org.scalatest.FunSuite
import org.scalatestplus.junit.AssertionsForJUnit

class ThriftIfaceTest extends FunSuite with AssertionsForJUnit {
  test("invalid thrift ifaces") {
    trait FakeThriftIface {
      def query(x: String): Future[String]
    }

    intercept[IllegalArgumentException] {
      ThriftMux.server.serveIface("localhost:*", new FakeThriftIface {
        def query(x: String) = Future.value(x)
      })
    }
  }
}
