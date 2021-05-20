package com.twitter.finagle.integration

import com.twitter.finagle.service.StatsFilter
import com.twitter.finagle.{Http, Memcached, Thrift, ThriftMux}
import org.scalatest.funsuite.AnyFunSuite

class StackServerTest extends AnyFunSuite {

  private val role = StatsFilter.role

  test("Http.withStack function") {
    assert(Http.server.stack.contains(role))
    val c: Http.Server = Http.server.withStack(_.remove(role))
    assert(!c.stack.contains(role))
  }

  test("ThriftMux.withStack function") {
    assert(ThriftMux.server.stack.contains(role))
    val c: ThriftMux.Server = ThriftMux.server.withStack(_.remove(role))
    assert(!c.stack.contains(role))
  }

  test("Thrift.withStack function") {
    assert(Thrift.server.stack.contains(role))
    val c: Thrift.Server = Thrift.server.withStack(_.remove(role))
    assert(!c.stack.contains(role))
  }

  test("Memcached.withStack function") {
    assert(Memcached.server.stack.contains(role))
    val c: Memcached.Server = Memcached.server.withStack(_.remove(role))
    assert(!c.stack.contains(role))
  }

}
