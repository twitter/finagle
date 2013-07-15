package com.twitter.finagle.thriftmux

import com.twitter.finagle.ThriftMux
import com.twitter.util.{Future, Try, Throw}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ThriftIfaceTest extends FunSuite {
  test("invalid thrift ifaces") {
    trait FakeThriftIface {
      def query(x: String): Future[String]
    }

    val res = Try {
      ThriftMux.serveIface(":*", new FakeThriftIface { def query(x: String) = Future.value(x) })
    }
    assert(res.isThrow)
  }
}