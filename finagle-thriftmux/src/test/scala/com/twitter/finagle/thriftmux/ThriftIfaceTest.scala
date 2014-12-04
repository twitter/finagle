package com.twitter.finagle.thriftmux

import com.twitter.finagle.ThriftMux
import com.twitter.util.{Future, Try}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}

@RunWith(classOf[JUnitRunner])
class ThriftIfaceTest extends FunSuite with AssertionsForJUnit {
  test("invalid thrift ifaces") {
    trait FakeThriftIface {
      def query(x: String): Future[String]
    }

    intercept[IllegalArgumentException] {
      ThriftMux.serveIface("localhost:*", new FakeThriftIface { def query(x: String) = Future.value(x) })
    }
  }
}
