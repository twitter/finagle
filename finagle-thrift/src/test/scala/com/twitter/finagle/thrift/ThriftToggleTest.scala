package com.twitter.finagle.thrift

import com.twitter.finagle.Thrift
import com.twitter.finagle.toggle.flag
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ThriftToggleTest extends FunSuite {

  test("Thrift client is configured to use Netty3 by default") {
    val client = Thrift.client
    val params = client.params

    assert(params[Thrift.ThriftImpl].transporter(params).toString == "Netty3Transporter")
  }

  test("Thrift client can be toggled to Netty4") {
    flag.overrides.let("com.twitter.finagle.thrift.UseNetty4", 1.0) {
      val params = Thrift.client.params
      assert(params[Thrift.ThriftImpl].transporter(params).toString == "Netty4Transporter")
    }
  }

  test("Thrift server is configured to use Netty3 by default") {
    val client = Thrift.server
    val params = client.params

    assert(params[Thrift.ThriftImpl].listener(params).toString == "Netty3Listener")
  }

  test("Thrift server can be toggled to Netty4") {
    flag.overrides.let("com.twitter.finagle.thrift.UseNetty4", 1.0) {
      val params = Thrift.server.params
      assert(params[Thrift.ThriftImpl].listener(params).toString == "Netty4Listener")
    }
  }
}
