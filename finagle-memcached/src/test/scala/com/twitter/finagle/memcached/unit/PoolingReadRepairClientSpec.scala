package com.twitter.finagle.memcached.unit

import com.twitter.finagle.{Service, ServiceException, ShardNotAvailableException}
import com.twitter.finagle.memcached._
import com.twitter.finagle.memcached.protocol._
import com.twitter.hashing.KeyHasher
import com.twitter.concurrent.Broker
import com.twitter.util.Future
import org.jboss.netty.buffer.ChannelBuffers
import org.specs.mock.Mockito
import org.specs.Specification
import scala.collection.mutable
import _root_.java.io.{BufferedReader, InputStreamReader}

object PoolingReadRepairClientSpec extends Specification with Mockito {
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
      pooled.withStrings.get("key")()                  must beSome("value")
    }

    "return the correct value and read-repair" in {
      partial.map.size mustEqual 1
      pooled.withStrings.get("foo")()                  must beSome("bar")
      partial.map.size mustEqual 2
    }
  }

}
