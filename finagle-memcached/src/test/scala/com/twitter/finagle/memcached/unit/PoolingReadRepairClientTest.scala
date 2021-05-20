package com.twitter.finagle.memcached.unit

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.memcached._
import com.twitter.util.{Await, Awaitable}
import org.scalatest.funsuite.AnyFunSuite

class PoolingReadRepairClientTest extends AnyFunSuite {

  val TimeOut = 15.seconds

  private def awaitResult[T](awaitable: Awaitable[T]): T = Await.result(awaitable, TimeOut)

  class Context {
    val full: MockClient = MockClient.fromStrings("key" -> "value", "foo" -> "bar")
    val partial: MockClient = MockClient.fromStrings("key" -> "value")
    val pooled: Client = new PoolingReadRepairClient(Seq(full, partial), 1, 1)
  }

  test("return the correct value") {
    val context = new Context
    import context._

    assert(awaitResult(pooled.withStrings.get("key")) == Some("value"))
  }

  test("return the correct value and read-repair") {
    val context = new Context
    import context._

    assert(partial.contents.size == 1)
    assert(awaitResult(pooled.withStrings.get("foo")) == Some("bar"))
    assert(partial.contents.size == 2)
  }

}
