package com.twitter.finagle.memcached.unit

import com.twitter.finagle.memcached._
import com.twitter.util.Await
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

class PoolingReadRepairClientSpec extends SpecificationWithJUnit with Mockito {
  var full: MockClient = null
  var partial: MockClient = null
  var pooled: Client = null
  var pooledNoRepair: Client = null

  def reset() = {
    full = new MockClient(Map("key" -> "value", "foo" -> "bar"))
    partial = new MockClient(Map("key" -> "value"))
    pooled = new PoolingReadRepairClient(Seq(full, partial), 1, 1)
  }
  reset()

  "PoolingReadRepairClient" should {
    "return the correct value" in {
      Await.result(pooled.withStrings.get("key"))                  must beSome("value")
    }

    "return the correct value and read-repair" in {
      partial.map.size mustEqual 1
      Await.result(pooled.withStrings.get("foo"))                  must beSome("bar")
      partial.map.size mustEqual 2
    }
  }
}
