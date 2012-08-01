package com.twitter.finagle.redis.integration

import org.specs.SpecificationWithJUnit
import com.twitter.finagle.Service
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.redis.Client
import com.twitter.finagle.redis.Redis
import com.twitter.finagle.stats.SummarizingStatsReceiver
import com.twitter.util.Future
import com.twitter.finagle.redis.util.RedisCluster
import com.twitter.finagle.redis.util.BytesToString

class ClientSpec extends SpecificationWithJUnit {

  "A BaseClient" should {
    /**
     * Note: This integration test requires a real Redis server to run.
     */
    var client: Client = null
    val foo = "foo".getBytes
    val bar = "bar".getBytes
    val baz = "baz".getBytes
    val boo = "boo".getBytes
    val moo = "moo".getBytes

    val stats = new SummarizingStatsReceiver

    doBefore {
      RedisCluster.start(1)
      val service = ClientBuilder()
        .codec(new Redis(stats))
        .hosts(RedisCluster.hostAddresses())
        .hostConnectionLimit(1)
        .build()
      client = Client(service)
    }

    doAfter {
      RedisCluster.stop()
    }

    "perform simple commands" in {

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

      "flush" in {
        client.set("foo", "bar".getBytes)()
        client.flushDB
        client.get("foo")() mustEqual None
      }

      "select" in {
        client.select(1)() mustEqual "OK"
      }

      "quit" in {
        client.quit()() mustEqual ()
      }

      "ttl" in {
        client.set("foo", bar)()
        client.expire("foo", 20)() mustEqual true
        client.ttl("foo")() map (_ must beLessThanOrEqualTo(20))
      }

    }


    "perform hash commands" in {

      "hash set and get" in {
        client.hSet(foo, bar, baz)()
        BytesToString(client.hGet(foo, bar)().get) mustEqual "baz"
        client.hGet(foo, boo)() mustEqual None
        client.hGet(bar, baz)() mustEqual None
      }

      "delete a single field" in {
        client.hSet(foo, bar, baz)()
        client.hDel("foo", Seq("bar"))() mustEqual 1
        client.hDel("foo", Seq("baz"))() mustEqual 0
      }

      "delete multiple fields" in {
        client.hSet(foo, bar, baz)()
        client.hSet(foo, boo, moo)()
        client.hDel("foo", Seq("bar", "boo"))() mustEqual 2
      }

      "get multiple values" in {
        client.hSet(foo, bar, baz)()
        client.hSet(foo, boo, moo)()
        BytesToString.fromList(
          client.hMGet("foo", Seq("bar", "boo"))().toList) mustEqual Seq("baz", "moo")
      }

      "get multiple values at once (deprecated)" in {
        client.hSet(foo, bar, baz)()
        client.hSet(foo, boo, moo)()
        BytesToString.fromTuples(
          client.hGetAll(foo)() toSeq) mustEqual Seq(("bar", "baz"), ("boo", "moo"))
      }

      "get multiple values at once" in {
        client.hSet(foo, bar, baz)()
        client.hSet(foo, boo, moo)()
        BytesToString.fromTuples(
          client.hGetAllAsPairs(foo)()) mustEqual Seq(("bar", "baz"), ("boo", "moo"))
      }

    }


    "perform sorted set commands" in {

      "add members and get scores" in {
        client.zAdd(foo, 10.5, bar)() mustEqual 1
        client.zAdd(foo, 20.1, baz)() mustEqual 1
        client.zScore(foo, bar)().get mustEqual 10.5
        client.zScore(foo, baz)().get mustEqual 20.1
      }

      "add members and get the zcount" in {
        client.zAdd(foo, 10, bar)() mustEqual 1
        client.zAdd(foo, 20, baz)() mustEqual 1
        client.zCount(foo, 0, 30)() mustEqual 2
        client.zCount(foo, 40, 50)() mustEqual 0
      }

      "get zRangeByScore (deprecated)" in {
        client.zAdd(foo, 10, bar)() mustEqual 1
        client.zAdd(foo, 20, baz)() mustEqual 1
        BytesToString.fromTuples(
          client.zRangeByScoreWithScores(foo, 0, 30, 0, 5)() toSeq) mustEqual Seq(("bar", "10"),
            ("baz", "20"))
      }

      "get the zRangeByScore" in {
        client.zAdd(foo, 10, bar)() mustEqual 1
        client.zAdd(foo, 20, baz)() mustEqual 1
        BytesToString.fromTuplesWithDoubles(
          client.zRangeByScore(foo, 0, 30, 0, 5)().asTuples) mustEqual Seq(("bar", 10),
            ("baz", 20))
      }

      "get cardinality and remove members" in {
        client.zAdd(foo, 10, bar)() mustEqual 1
        client.zAdd(foo, 20, baz)() mustEqual 1
        client.zCard(foo)() mustEqual 2
        client.zRem(foo, Seq(bar, baz))() mustEqual 2
      }

      "get zRevRange" in {
        client.zAdd(foo, 10, bar)() mustEqual 1
        client.zAdd(foo, 20, baz)() mustEqual 1
        BytesToString.fromList(
          client.zRevRange(foo, 0, -1)().toList) mustEqual Seq("baz", "bar")
      }

      "get zRevRangeByScoreWithScores (deprecated)" in {
        client.zAdd(foo, 10, bar)() mustEqual 1
        client.zAdd(foo, 20, baz)() mustEqual 1
        BytesToString.fromTuples(
          client.zRevRangeByScoreWithScores(foo, 0, 10, 0, 1)() toSeq) mustEqual Seq(("bar", "10"))
        client.zRevRangeByScoreWithScores(foo, 0, 0, 0, 1)() mustEqual Map()
      }

      "get zRevRangeByScoreWithScores" in {
        client.zAdd(foo, 10, bar)() mustEqual 1
        client.zAdd(foo, 20, baz)() mustEqual 1
        BytesToString.fromTuplesWithDoubles(
          client.zRevRangeByScore(foo, 0, 10, 0, 1)().asTuples) mustEqual Seq(("bar", 10))
        client.zRevRangeByScore(foo, 0, 0, 0, 1)().asTuples == Seq()
      }

    }

    "perform list commands" in {
      "push members and pop them off" in {
        val key = "push"
        client.lPush(key, List(bar))() mustEqual 1
        client.lPush(key, List(baz))() mustEqual 2
        client.lPop(key)() map (BytesToString(_) mustEqual "baz")
        client.lPop(key)() map (BytesToString(_) mustEqual "bar")
      }

      "push members and measure their length, then pop them off" in {
        val key = "llen"
        client.lLen(key)() mustEqual 0
        client.lPush(key, List(bar))() mustEqual 1
        client.lLen(key)() mustEqual 1
        client.lPush(key, List(baz))() mustEqual 2
        client.lLen(key)() mustEqual 2
        client.lPop(key)() map (BytesToString(_) mustEqual "baz")
        client.lLen(key)() mustEqual 1
        client.lPop(key)() map (BytesToString(_) mustEqual "bar")
        client.lLen(key)() mustEqual 0
      }

      "push members and index them, then pop them off, and index them" in {
        val key = "lindex"
        client.lIndex(key, 0)() mustEqual None
        client.lPush(key, List(bar))() mustEqual 1
        client.lIndex(key, 0)() map (BytesToString(_) mustEqual "bar")
        client.lPush(key, List(baz))() mustEqual 2
        client.lIndex(key, 0)() map (BytesToString(_) mustEqual "baz")
        client.lPop(key)() map (BytesToString(_) mustEqual "baz")
        client.lIndex(key, 0)() map (BytesToString(_) mustEqual "bar")
        client.lPop(key)() map (BytesToString(_) mustEqual "bar")
      }

      "push a member, then insert some values, then pop them off" in {
        val key = "linsert"
        client.lPush(key, List(bar))() mustEqual 1
        client.lInsertAfter(key, bar, baz)
        client.lInsertBefore(key, bar, moo)
        client.lPop(key)() map (BytesToString(_) mustEqual "moo")
        client.lPop(key)() map (BytesToString(_) mustEqual "bar")
        client.lPop(key)() map (BytesToString(_) mustEqual "baz")
      }

      "push members and remove one, then pop the other off" in {
        val key = "lremove"
        client.lPush(key, List(bar))() mustEqual 1
        client.lPush(key, List(baz))() mustEqual 2
        client.lRem(key, 1, baz)() mustEqual 1
        client.lPop(key)() map (BytesToString(_) mustEqual "bar")
      }

      "push members and set one, then pop them off" in {
        val key = "lset"
        client.lPush(key, List(bar))() mustEqual 1
        client.lPush(key, List(baz))() mustEqual 2
        client.lSet(key, 0, moo)()
        client.lPop(key)() map (BytesToString(_) mustEqual "moo")
        client.lPop(key)() map (BytesToString(_) mustEqual "bar")
      }

      "push members examine the entire range, then pop them off" in {
        val key = "lrange"
        client.lPush(key, List(bar))() mustEqual 1
        client.lPush(key, List(baz))() mustEqual 2
        client.lRange(key, 0, -1)() map (BytesToString(_)) mustEqual List("baz", "bar")
        client.lPop(key)() map (BytesToString(_) mustEqual "baz")
        client.lPop(key)() map (BytesToString(_) mustEqual "bar")
      }

      "push members, then pop them off of the other side, queue style" in {
        val key = "rpop"
        client.lPush(key, List(bar))() mustEqual 1
        client.lPush(key, List(baz))() mustEqual 2
        client.rPop(key)() map (BytesToString(_) mustEqual "bar")
        client.rPop(key)() map (BytesToString(_) mustEqual "baz")
      }

      "push members and then pop them off, except from the other side." in {
        val key = "rpop"
        client.rPush(key, List(bar))() mustEqual 1
        client.rPush(key, List(baz))() mustEqual 2
        client.rPop(key)() map (BytesToString(_) mustEqual "baz")
        client.rPop(key)() map (BytesToString(_) mustEqual "bar")
      }

      "push members, trimming as we go.  then pop off the two remaining." in {
        val key = "ltrim"
        client.lPush(key, List(bar))() mustEqual 1
        client.lPush(key, List(baz))() mustEqual 2
        client.lPush(key, List(boo))() mustEqual 3
        client.lTrim(key, 0, 1)()
        client.lPush(key, List(moo))() mustEqual 3
        client.lTrim(key, 0, 1)()
        client.rPop(key)() map (BytesToString(_) mustEqual "boo")
        client.rPop(key)() map (BytesToString(_) mustEqual "moo")
      }
    }

  }

}