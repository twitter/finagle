package com.twitter.finagle.integration

import com.twitter.finagle.service.StatsFilter
import com.twitter.finagle.{Http, Memcached, Mysql, Redis, Thrift, ThriftMux}
import org.scalatest.funsuite.AnyFunSuite

class StackClientTest extends AnyFunSuite {

  private val role = StatsFilter.role

  test("Http.withStack function") {
    assert(Http.client.stack.contains(role))
    val c: Http.Client = Http.client.withStack(_.remove(role))
    assert(!c.stack.contains(role))
  }

  test("ThriftMux.withStack function") {
    assert(ThriftMux.client.stack.contains(role))
    val c: ThriftMux.Client = ThriftMux.client.withStack(_.remove(role))
    assert(!c.stack.contains(role))
  }

  test("Thrift.withStack function") {
    assert(Thrift.client.stack.contains(role))
    val c: Thrift.Client = Thrift.client.withStack(_.remove(role))
    assert(!c.stack.contains(role))
  }

  test("Memcached.withStack function") {
    assert(Memcached.client.stack.contains(role))
    val c: Memcached.Client = Memcached.client.withStack(_.remove(role))
    assert(!c.stack.contains(role))
  }

  test("Mysql.withStack function") {
    assert(Mysql.client.stack.contains(role))
    val c: Mysql.Client = Mysql.client.withStack(_.remove(role))
    assert(!c.stack.contains(role))
  }

  test("Redis.withStack function") {
    assert(Redis.client.stack.contains(role))
    val c: Redis.Client = Redis.client.withStack(_.remove(role))
    assert(!c.stack.contains(role))
  }

}
