package com.twitter.finagle.redis.integration

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.{Redis, ClientError, TransactionalClient}
import com.twitter.finagle.redis.util.{BytesToString, RedisCluster, ReplyFormat}
import com.twitter.finagle.Service
import com.twitter.finagle.stats.SummarizingStatsReceiver
import com.twitter.util.{Future, RandomSocket}
import org.specs.SpecificationWithJUnit

class ClientSpec extends SpecificationWithJUnit {

  "A BaseClient" should {
    /**
     * Note: This integration test requires a real Redis server to run.
     */
    var client: TransactionalClient = null
    val foo = "foo".getBytes
    val bar = "bar".getBytes
    val baz = "baz".getBytes
    val boo = "boo".getBytes
    val moo = "moo".getBytes

    val stats = new SummarizingStatsReceiver

    doBefore {
      RedisCluster.start(1)
      client = TransactionalClient(
        ClientBuilder()
         .codec(new Redis(stats))
         .hosts(RedisCluster.hostAddresses())
         .hostConnectionLimit(1)
         .buildFactory())
    }

    doAfter {
      RedisCluster.stop()
      client.release
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

      "keys" in {
        client.set("foo", bar)()
        client.set("moo", boo)()
        BytesToString.fromList(client.keys("")().toList) must throwA[ClientError]
        BytesToString.fromList(client.keys("*oo")().toList) mustEqual Seq("moo", "foo")
        BytesToString.fromList(client.keys("*z*")().toList) mustEqual Seq()
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

      "get fields in a hash" in {
        client.hSet(foo, bar, baz)()
        client.hSet(foo, boo, moo)()
        BytesToString.fromList(
          client.hKeys("foo")().toList) mustEqual Seq("bar", "boo")
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


    "perform commands as a transaction" in {
      "set and get transaction" in {
        val txResult = client.transaction(Seq(Set("foo", bar), Set("baz", boo)))()
        ReplyFormat.toString(txResult.toList) mustEqual Seq("OK", "OK")
      }

      "hash set and multi get transaction" in {
        val txResult = client.transaction(Seq(HSet(foo, bar, baz), HSet(foo, boo, moo),
          HMGet("foo", Seq("bar", "boo"))))()
        ReplyFormat.toString(txResult.toList) mustEqual Seq("1", "1", "baz", "moo")
      }

      "key command on incorrect data type" in {
        val txResult = client.transaction(Seq(HSet(foo, boo, moo),
          Get("foo"), HDel("foo", List(boo))))()
        txResult.toList mustEqual Seq(IntegerReply(1),
          ErrorReply("ERR Operation against a key holding the wrong kind of value"),
          IntegerReply(1))
      }

      "fail after a watched key is modified" in {
        client.set("foo", bar)()
        client.watch(Seq(foo))()
        client.set("foo", boo)()
        client.transaction(Seq(Get("foo")))() must throwA[ClientError]
      }

      "watch then unwatch a key" in {
        client.set("foo", bar)()
        client.watch(Seq(foo))()
        client.set("foo", boo)()
        client.unwatch()()
        val txResult = client.transaction(Seq(Get("foo")))()
        ReplyFormat.toString(txResult.toList) mustEqual Seq("boo")
      }

      "set followed by get on the same key" in {
        val txResult = client.transaction(Seq(Set("foo", bar), Get("foo")))()
        ReplyFormat.toString(txResult.toList) mustEqual Seq("OK", "bar")
      }

    }

  }

}