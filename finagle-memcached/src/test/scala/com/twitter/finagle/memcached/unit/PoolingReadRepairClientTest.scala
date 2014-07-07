package com.twitter.finagle.memcached.unit

import com.twitter.finagle.memcached._
import com.twitter.util.Await
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PoolingReadRepairClientTest extends FunSuite with BeforeAndAfter {
  var full: MockClient = null
  var partial: MockClient = null
  var pooled: Client = null
  var pooledNoRepair: Client = null

  before {
    full = new MockClient(Map("key" -> "value", "foo" -> "bar"))
    partial = new MockClient(Map("key" -> "value"))
    pooled = new PoolingReadRepairClient(Seq(full, partial), 1, 1)
  }

  test("return the correct value") {
    assert(Await.result(pooled.withStrings.get("key")) === Some("value"))
  }

  test("return the correct value and read-repair") {
    assert(partial.map.size                            === 1)
    assert(Await.result(pooled.withStrings.get("foo")) === Some("bar"))
    assert(partial.map.size                            === 2)
  }

}
