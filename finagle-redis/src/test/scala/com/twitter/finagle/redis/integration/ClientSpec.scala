package com.twitter.finagle.redis.integration

import org.specs.SpecificationWithJUnit
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.{Redis, ClientError, TransactionalClient}
import com.twitter.finagle.redis.util.{BytesToString, RedisCluster, ReplyFormat}
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

      "add members and zIncr, then zIncr a nonmember" in {
        client.zAdd(foo, 10, bar)() mustEqual 1
        client.zIncrBy(foo, 10, bar)() mustEqual Some(20)
        client.zIncrBy(foo, 10, baz)() mustEqual Some(10)
      }

      "get zRange" in {
        client.zAdd(foo, 10, bar)() mustEqual 1
        client.zAdd(foo, 20, baz)() mustEqual 1
        client.zAdd(foo, 30, boo)() mustEqual 1
        BytesToString.fromList(
          client.zRange(foo, 0, -1)().toList) mustEqual List("bar", "baz", "boo")
        BytesToString.fromList(
          client.zRange(foo, 2, 3)().toList) mustEqual List("boo")
        BytesToString.fromList(
          client.zRange(foo, -2, -1)().toList) mustEqual List("baz", "boo")
      }

      "get zRank" in {
        client.zAdd(foo, 10, bar)() mustEqual 1
        client.zAdd(foo, 20, baz)() mustEqual 1
        client.zAdd(foo, 30, boo)() mustEqual 1
        client.zRank(foo, boo)() mustEqual Some(2)
        client.zRank(foo, moo)() mustEqual None
      }

      "get zRemRangeByRank" in {
        client.zAdd(foo, 10, bar)() mustEqual 1
        client.zAdd(foo, 20, baz)() mustEqual 1
        client.zAdd(foo, 30, boo)() mustEqual 1
        client.zRemRangeByRank(foo, 0, 1)() mustEqual 2
        BytesToString.fromList(
          client.zRange(foo, 0, -1)().toList) mustEqual List("boo")
      }

      "get zRemRangeByScore" in {
        client.zAdd(foo, 10, bar)() mustEqual 1
        client.zAdd(foo, 20, baz)() mustEqual 1
        client.zAdd(foo, 30, boo)() mustEqual 1
        client.zRemRangeByScore(foo, 10, 20)() mustEqual 2
        BytesToString.fromList(
          client.zRange(foo, 0, -1)().toList) mustEqual List("boo")
      }

      "get zRevRank" in {
        client.zAdd(foo, 10, bar)() mustEqual 1
        client.zAdd(foo, 20, baz)() mustEqual 1
        client.zAdd(foo, 30, boo)() mustEqual 1
        client.zRevRank(foo, boo)() mustEqual Some(0)
        client.zRevRank(foo, moo)() mustEqual None
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

    "perform set commands" in {
      "add members to a set, then pop them off." in {
        val key = "pushpop"
        client.sAdd(key, List(bar))() mustEqual 1
        client.sAdd(key, List(baz))() mustEqual 1
        client.sPop(key)()
        client.sPop(key)()
      }

      "add members to a set, then pop them off, counting them." in {
        val key = "scard"
        client.sAdd(key, List(bar))() mustEqual 1
        client.sCard(key)() mustEqual 1
        client.sAdd(key, List(baz))() mustEqual 1
        client.sCard(key)() mustEqual 2
        client.sPop(key)()
        client.sCard(key)() mustEqual 1
        client.sPop(key)()
        client.sCard(key)() mustEqual 0
      }

      "add members to a set, look for some, pop them off, look for some again." in {
        val key = "members"
        client.sAdd(key, List(bar))() mustEqual 1
        client.sIsMember(key, bar)() mustEqual true
        client.sIsMember(key, baz)() mustEqual false
        client.sAdd(key, List(baz))() mustEqual 1
        client.sIsMember(key, bar)() mustEqual true
        client.sIsMember(key, baz)() mustEqual true
        client.sPop(key)()
        client.sPop(key)()
        client.sIsMember(key, bar)() mustEqual false
        client.sIsMember(key, baz)() mustEqual false
      }

      "add members to a set, then examine them, then pop them off, then examien them again." in {
        val key = "members"
        client.sAdd(key, List(bar))() mustEqual 1
        client.sAdd(key, List(baz))() mustEqual 1
        client.sMembers(key)() map (new String(_)) mustEqual Set("bar", "baz")
        client.sPop(key)()
        client.sPop(key)()
        client.sMembers(key)() mustEqual Set()
      }

      "add members to a set, then remove them." in {
        val key = "members"
        client.sAdd(key, List(bar))() mustEqual 1
        client.sAdd(key, List(baz))() mustEqual 1
        client.sRem(key, List(bar))() mustEqual 1
        client.sRem(key, List(baz))() mustEqual 1
        client.sRem(key, List(baz))() mustEqual 0
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
