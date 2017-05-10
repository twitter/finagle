package com.twitter.finagle.thrift

import com.twitter.finagle.Thrift
import java.net.SocketAddress
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ThriftToggleTest extends FunSuite {

  test("Thrift client is configured to use Netty3 by default") {
    val client = Thrift.client
    val params = client.params
    val addr = new SocketAddress { }

    assert(params[Thrift.ThriftImpl].transporter(params)(addr).toString == "Netty3Transporter")
  }

  test("Thrift server is configured to use Netty3 by default") {
    val client = Thrift.server
    val params = client.params

    assert(params[Thrift.ThriftImpl].listener(params).toString == "Netty3Listener")
  }
}
