package com.twitter.finagle.integration

import com.twitter.finagle.service.Retries
import com.twitter.finagle.service.StatsFilter
import com.twitter.finagle.Http
import com.twitter.finagle.Memcached
import com.twitter.finagle.Mysql
import com.twitter.finagle.PostgreSql
import com.twitter.finagle.Redis
import com.twitter.finagle.Thrift
import com.twitter.finagle.ThriftMux
import com.twitter.finagle.client.StackBasedClient
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

  def testThatRetryBudgetIsShared(newClient: () => StackBasedClient[_, _], protocol: String): Unit =
    test(s"$protocol clients shipped with retry-budget included") {
      // every new clients gets new retry budget
      assert(
        newClient().params[Retries.Budget].retryBudget ne newClient()
          .params[Retries.Budget].retryBudget)

      // single client shares the retry budget between it's modules
      val client = newClient()
      assert(client.params[Retries.Budget].retryBudget eq client.params[Retries.Budget].retryBudget)
    }

  testThatRetryBudgetIsShared(Thrift.client _, "Thrift")
  testThatRetryBudgetIsShared(ThriftMux.client _, "ThriftMux")
  testThatRetryBudgetIsShared(Http.client _, "Http")
  testThatRetryBudgetIsShared(Memcached.client _, "Memcached")
  testThatRetryBudgetIsShared(Redis.client _, "Redis")
  testThatRetryBudgetIsShared(Mysql.client _, "Mysql")
  testThatRetryBudgetIsShared(PostgreSql.client _, "PostgreSql")
}
