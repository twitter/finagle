package com.twitter.finagle.integration

import com.twitter.finagle.{Http, Memcached, Mysql, Redis, Thrift, ThriftMux}
import com.twitter.finagle.stack.Endpoint
import org.scalatest.FunSuite

class StackClientTest extends FunSuite {

  test("Http.withStack function") {
    assert(Http.client.stack.contains(Endpoint))
    val c: Http.Client = Http.client.withStack(_.remove(Endpoint))
    assert(c.stack.contains(Endpoint))
  }

  test("ThriftMux.withStack function") {
    assert(ThriftMux.client.stack.contains(Endpoint))
    val c: ThriftMux.Client = ThriftMux.client.withStack(_.remove(Endpoint))
    assert(c.stack.contains(Endpoint))
  }

  test("Thrift.withStack function") {
    assert(Thrift.client.stack.contains(Endpoint))
    val c: Thrift.Client = Thrift.client.withStack(_.remove(Endpoint))
    assert(c.stack.contains(Endpoint))
  }

  test("Memcached.withStack function") {
    assert(Memcached.client.stack.contains(Endpoint))
    val c: Memcached.Client = Memcached.client.withStack(_.remove(Endpoint))
    assert(c.stack.contains(Endpoint))
  }

  test("Mysql.withStack function") {
    assert(Mysql.client.stack.contains(Endpoint))
    val c: Mysql.Client = Mysql.client.withStack(_.remove(Endpoint))
    assert(c.stack.contains(Endpoint))
  }

  test("Redis.withStack function") {
    assert(Redis.client.stack.contains(Endpoint))
    val c: Redis.Client = Redis.client.withStack(_.remove(Endpoint))
    assert(c.stack.contains(Endpoint))
  }

}
