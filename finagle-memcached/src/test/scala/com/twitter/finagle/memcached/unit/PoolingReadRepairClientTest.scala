package com.twitter.finagle.memcached.unit

import com.twitter.finagle.memcached._
import com.twitter.util.Await
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class PoolingReadRepairClientTest extends FunSuite {

  class Context {
    val full: MockClient = new MockClient(Map("key" -> "value", "foo" -> "bar"))
    val partial: MockClient = new MockClient(Map("key" -> "value"))
    val pooled: Client = new PoolingReadRepairClient(Seq(full, partial), 1, 1)
  }

  test("return the correct value") {
    val context = new Context
    import context._

    assert(Await.result(pooled.withStrings.get("key")) == Some("value"))
  }

  test("return the correct value and read-repair") {
    val context = new Context
    import context._

    assert(partial.map.size                            == 1)
    assert(Await.result(pooled.withStrings.get("foo")) == Some("bar"))
    assert(partial.map.size                            == 2)
  }

}
