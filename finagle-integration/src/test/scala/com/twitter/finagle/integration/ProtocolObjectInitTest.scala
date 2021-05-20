package com.twitter.finagle.integration

import com.twitter.finagle._
import org.scalatest.funsuite.AnyFunSuite

/**
 * We adopt the convention that all of our protocol objects
 * use deferred evaluation for the client/server aliases to
 * avoid initialization order bugs.
 */
class ProtocolObjectInitTest extends AnyFunSuite {

  def assertNonNull(obj: Any) = assert(obj != null)

  test("Http") {
    Http.Client()
    assertNonNull(Http.client.stack)
    assertNonNull(Http.client.params)

    Http.Server()
    assertNonNull(Http.server.stack)
    assertNonNull(Http.server.params)
  }

  test("Mux") {
    Mux.Client()
    assertNonNull(Mux.client.stack)
    assertNonNull(Mux.client.params)

    Mux.Server()
    assertNonNull(Mux.server.stack)
    assertNonNull(Mux.server.params)
  }

  test("ThriftMux") {
    ThriftMux.Client()
    assertNonNull(ThriftMux.client.muxer)

    ThriftMux.Server()
    assertNonNull(ThriftMux.server.stack)
    assertNonNull(ThriftMux.server.params)
  }

  test("Thrift") {
    Thrift.Client()
    assertNonNull(Thrift.client.stack)
    assertNonNull(Thrift.client.params)

    Thrift.Server()
    assertNonNull(Thrift.server.stack)
    assertNonNull(Thrift.server.params)
  }

  test("Memcached") {
    Memcached.Client()
    assertNonNull(Memcached.client.stack)
    assertNonNull(Memcached.client.params)

    Memcached.Server()
    assertNonNull(Memcached.server.stack)
    assertNonNull(Memcached.server.params)
  }

  test("Redis") {
    Redis.Client()
    assertNonNull(Redis.client.stack)
    assertNonNull(Redis.client.params)
  }

  test("Mysql") {
    Mysql.Client()
    assertNonNull(Mysql.client.stack)
    assertNonNull(Mysql.client.params)
  }
}
