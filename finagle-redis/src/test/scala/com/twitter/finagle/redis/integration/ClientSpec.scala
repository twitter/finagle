package com.twitter.finagle.redis.integration

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.redis.{Client, Redis}
import com.twitter.finagle.redis.util.{RedisCluster, BytesToString}
import com.twitter.finagle.Service
import com.twitter.util.{Future, RandomSocket}
import org.specs.Specification


object ClientSpec extends Specification {

  "BaseClient" should {
    /**
     * Note: This integration test requires a real Redis server to run.
     */
    var client: Client = null

    doBefore {
      RedisCluster.start(1)
      val service = ClientBuilder()
        .codec(new Redis())
        .hosts(RedisCluster.hostAddresses())
        .hostConnectionLimit(1)
        .build()
      client = Client(service)
    }

    doAfter {
      RedisCluster.stop()
    }

    "simple client" in {

      "append" in {
        client.del(Seq("foo"))()
        client.set("foo", "bar".getBytes)()
        client.append("foo", "baz".getBytes)() mustEqual 6
      }

      "decrBy" in {
        client.del(Seq("foo"))()
        client.set("foo", "21".getBytes)()
        client.decrBy("foo", 2)() mustEqual 19
      }

      "exists" in {
        client.del(Seq("foo"))()
        client.set("foo", "bar".getBytes)()
        client.exists("foo")() mustEqual true
      }

      "get range" in {
        client.del(Seq("foo"))()
        client.set("foo", "boing".getBytes)()
        BytesToString(client.getRange("foo", 0, 2)().get) mustEqual "boi"
      }

      "set & get" in {
        client.del(Seq("foo"))()
        client.get("foo")() mustEqual None
        client.set("foo", "bar".getBytes)()
        BytesToString(client.get("foo")().get) mustEqual "bar"
      }

    }

  }

}