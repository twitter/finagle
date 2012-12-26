package com.twitter.finagle.redis.integration

import org.jboss.netty.buffer.ChannelBuffer
import org.specs.SpecificationWithJUnit
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.redis.Redis
import com.twitter.finagle.redis.TransactionalClient
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.stats.SummarizingStatsReceiver
import com.twitter.util.Future
import com.twitter.finagle.redis.util.StringToChannelBuffer
import com.twitter.finagle.redis.util.RedisCluster
import com.twitter.finagle.redis.util.CBToString
import scala.collection.{Set => CollectionSet}
import com.twitter.finagle.redis.util.ReplyFormat
import com.twitter.finagle.redis.ClientError


class ClientSpec extends SpecificationWithJUnit {

  "A BaseClient" should {
    /**
     * Note: This integration test requires a real Redis server to run.
     */
    var client: TransactionalClient = null
    val foo = StringToChannelBuffer("foo")
    val bar = StringToChannelBuffer("bar")
    val baz = StringToChannelBuffer("baz")
    val boo = StringToChannelBuffer("boo")
    val moo = StringToChannelBuffer("moo")

    doBefore {
      RedisCluster.start()
      client = TransactionalClient(
        ClientBuilder()
         .codec(new Redis())
         .hosts(RedisCluster.hostAddresses())
         .hostConnectionLimit(1)
         .buildFactory())
      client.flushDB()()
    }

    doAfter {
      client.release
      RedisCluster.stop()
    }

    "perform simple commands" in {
      "append" in {
        client.set(foo, bar)()
        client.append(foo, baz)() mustEqual 6
      }

      "decrBy" in {
        client.set(foo, StringToChannelBuffer("21"))()
        client.decrBy(foo, 2)() mustEqual 19
      }

      "del" in {
        client.set(foo, bar)()
        client.del(Seq(foo))()
        client.get(foo)() mustEqual None
      }

      "exists" in {
        client.set(foo, bar)()
        client.exists(foo)() mustEqual true
      }

      "get range" in {
        client.set(foo, StringToChannelBuffer("boing"))()
        CBToString(client.getRange(foo, 0, 2)().get) mustEqual "boi"
      }

      "set & get" in {
        client.get(foo)() mustEqual None
        client.set(foo, bar)()
        CBToString(client.get(foo)().get) mustEqual "bar"
      }

      "flush" in {
        client.set(foo, bar)()
        client.flushDB()()
        client.get(foo)() mustEqual None
      }

      "select" in {
        client.select(1)() mustEqual ()
      }

      "quit" in {
        client.quit()() mustEqual ()
      }

      "ttl" in {
        client.set(foo, bar)()
        client.expire(foo, 20)() mustEqual true
        client.ttl(foo)() map (_ must beLessThanOrEqualTo(20L))
      }

      // Once the scan/hscan pull request gets merged into Redis master,
      // the tests can be uncommented.
      // "scan" in {
      //   client.set(foo, bar)()
      //   client.set(baz, boo)()
      //   val res = client.scan(0, None, None)()
      //   CBToString(res(1)) mustEqual "baz"
      //   val withCount = client.scan(0, Some(10), None)()
      //   CBToString(withCount(0)) mustEqual "0"
      //   CBToString(withCount(1)) mustEqual "baz"
      //   CBToString(withCount(2)) mustEqual "foo"
      //   val pattern = StringToChannelBuffer("b*")
      //   val withPattern = client.scan(0, None, Some(pattern))
      //   CBToString(withCount(0)) mustEqual "0"
      //   CBToString(withCount(1)) mustEqual "baz"
      // }
    }

    "perform string commands" in {
      "bit operations" in {
        client.bitCount(foo)() mustEqual 0L
        client.getBit(foo, 0)() mustEqual 0L
        client.setBit(foo, 0, 1)() mustEqual 0L
        client.getBit(foo, 0)() mustEqual 1L
        client.setBit(foo, 0, 0)() mustEqual 1L

        client.setBit(foo, 2, 1)() mustEqual 0L
        client.setBit(foo, 3, 1)() mustEqual 0L

        client.setBit(foo, 8, 1)() mustEqual 0L
        client.bitCount(foo)() mustEqual 3L
        client.bitCount(foo, Some(0), Some(0))() mustEqual 2L
        client.setBit(foo, 8, 0)() mustEqual 1L

        client.setBit(bar, 0, 1)() mustEqual 0L
        client.setBit(bar, 3, 1)() mustEqual 0L

        client.bitOp(BitOp.And, baz, Seq(foo, bar))() mustEqual 2L
        client.bitCount(baz)() mustEqual 1L
        client.getBit(baz, 0)() mustEqual 0L
        client.getBit(baz, 3)() mustEqual 1L

        client.bitOp(BitOp.Or, baz, Seq(foo, bar))() mustEqual 2L
        client.bitCount(baz)() mustEqual 3L
        client.getBit(baz, 0)() mustEqual 1L
        client.getBit(baz, 1)() mustEqual 0L

        client.bitOp(BitOp.Xor, baz, Seq(foo, bar))() mustEqual 2L
        client.bitCount(baz)() mustEqual 2L
        client.getBit(baz, 0)() mustEqual 1L
        client.getBit(baz, 1)() mustEqual 0L

        client.bitOp(BitOp.Not, baz, Seq(foo))() mustEqual 2L
        client.bitCount(baz)() mustEqual 14
        client.getBit(baz, 0)() mustEqual 1
        client.getBit(baz, 2)() mustEqual 0
        client.getBit(baz, 4)() mustEqual 1
      }

      "getSet" in {
        client.getSet(foo, bar)() mustEqual None
        client.get(foo)() mustEqual Some(bar)
        client.getSet(foo, baz)() mustEqual Some(bar)
        client.get(foo)() mustEqual Some(baz)
      }

      "incr / incrBy" in {
        client.incr(foo)() mustEqual 1L
        client.incrBy(foo, 10L)() mustEqual 11L
        client.incrBy(bar, 10L)() mustEqual 10L
        client.incr(bar)() mustEqual 11L
      }

      "mGet / mSet / mSetNx" in {
        client.mSet(Map(foo -> bar, bar -> baz))()
        client.mGet(Seq(foo, bar, baz))() mustEqual Seq(Some(bar), Some(baz), None)
        client.mSetNx(Map(foo -> bar, baz -> foo, boo -> moo))() mustEqual false
        client.mSetNx(Map(baz -> foo, boo -> moo))() mustEqual true
        client.mGet(Seq(baz, boo))() mustEqual Seq(Some(foo), Some(moo))
      }

      "set variations" in {
        client.pSetEx(foo, 10000L, bar)()
        client.get(foo)() mustEqual Some(bar)
        client.ttl(foo)() map (_ must beLessThanOrEqualTo(10L))

        client.setEx(bar, 10L, foo)()
        client.get(bar)() mustEqual Some(foo)
        client.ttl(bar)() map (_ must beLessThanOrEqualTo(10L))

        client.setNx(baz, foo)() mustEqual true
        client.setNx(baz, bar)() mustEqual false

        client.setRange(baz, 1, baz)() mustEqual 4L
        client.get(baz)() mustEqual Some(StringToChannelBuffer("fbaz"))
      }

      "strlen" in {
        client.strlen(foo)() mustEqual 0L
        client.set(foo, bar)()
        client.strlen(foo)() mustEqual 3L
      }
    }

    "perform hash commands" in {

      "hash set and get" in {
        client.hSet(foo, bar, baz)()
        CBToString(client.hGet(foo, bar)().get) mustEqual "baz"
        client.hGet(foo, boo)() mustEqual None
        client.hGet(bar, baz)() mustEqual None
      }

      "delete a single field" in {
        client.hSet(foo, bar, baz)()
        client.hDel(foo, Seq(bar))() mustEqual 1
        client.hDel(foo, Seq(baz))() mustEqual 0
      }

      "delete multiple fields" in {
        client.hSet(foo, bar, baz)()
        client.hSet(foo, boo, moo)()
        client.hDel(foo, Seq(bar, boo))() mustEqual 2
      }

      "get multiple values" in {
        client.hSet(foo, bar, baz)()
        client.hSet(foo, boo, moo)()
        CBToString.fromList(
          client.hMGet(foo, Seq(bar, boo))().toList) mustEqual Seq("baz", "moo")
      }

      "set multiple values" in {
        client.hMSet(foo, Map(baz -> bar, moo -> boo))()
        CBToString.fromList(
          client.hMGet(foo, Seq(baz, moo))().toList) mustEqual Seq("bar", "boo")
      }

      "get multiple values at once" in {
        client.hSet(foo, bar, baz)()
        client.hSet(foo, boo, moo)()
        CBToString.fromTuples(
          client.hGetAll(foo)()) mustEqual Seq(("bar", "baz"), ("boo", "moo"))
      }

      // "hscan" in {
      //   client.hSet(foo, bar, baz)()
      //   client.hSet(foo, boo, moo)()
      //   val res = client.hScan(foo, 0, None, None)()
      //   CBToString(res(1)) mustEqual "bar"
      //   val withCount = client.hScan(foo, 0, Some(2), None)()
      //   CBToString(withCount(0)) mustEqual "0"
      //   CBToString(withCount(1)) mustEqual "bar"
      //   CBToString(withCount(2)) mustEqual "boo"
      //   val pattern = StringToChannelBuffer("b*")
      //   val withPattern = client.hScan(foo, 0, None, Some(pattern))
      //   CBToString(withCount(0)) mustEqual "0"
      //   CBToString(withCount(1)) mustEqual "bar"
      //   CBToString(withCount(2)) mustEqual "boo"
      // }

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
        client.zCount(foo, ZInterval(0), ZInterval(30))() mustEqual 2
        client.zCount(foo, ZInterval(40), ZInterval(50))() mustEqual 0
      }

      "get the zRangeByScore" in {
        client.zAdd(foo, 10, bar)() mustEqual 1
        client.zAdd(foo, 20, baz)() mustEqual 1
        CBToString.fromTuplesWithDoubles(
          client.zRangeByScore(foo, ZInterval(0), ZInterval(30), true,
            Some(Limit(0, 5)))().asTuples) mustEqual Seq(("bar", 10), ("baz", 20))
        client.zRangeByScore(foo, ZInterval(30), ZInterval(0), true,
          Some(Limit(0, 5)))().asTuples mustEqual Seq()
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
        CBToString.fromList(
          client.zRevRange(foo, 0, -1)().toList) mustEqual Seq("baz", "bar")
      }

      "get zRevRangeByScore" in {
        client.zAdd(foo, 10, bar)() mustEqual 1
        client.zAdd(foo, 20, baz)() mustEqual 1
        CBToString.fromTuplesWithDoubles(
          client.zRevRangeByScore(foo, ZInterval(10), ZInterval(0), true,
            Some(Limit(0, 1)))().asTuples) mustEqual Seq(("bar", 10))
        client.zRevRangeByScore(foo, ZInterval(0), ZInterval(10), true,
            Some(Limit(0, 1)))().asTuples mustEqual Seq()
        client.zRevRangeByScore(foo, ZInterval(0), ZInterval(0), true,
          Some(Limit(0, 1)))().asTuples mustEqual Seq()
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
        CBToString.fromList(client.zRange(foo, 0, -1)().toList) mustEqual List("bar", "baz", "boo")
        CBToString.fromList(
          client.zRange(foo, 2, 3)().toList) mustEqual List("boo")
        CBToString.fromList(
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
        CBToString.fromList(
          client.zRange(foo, 0, -1)().toList) mustEqual List("boo")
      }

      "get zRemRangeByScore" in {
        client.zAdd(foo, 10, bar)() mustEqual 1
        client.zAdd(foo, 20, baz)() mustEqual 1
        client.zAdd(foo, 30, boo)() mustEqual 1
        client.zRemRangeByScore(foo, ZInterval(10), ZInterval(20))() mustEqual 2
        CBToString.fromList(
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
        val key = StringToChannelBuffer("push")
        client.lPush(key, List(bar))() mustEqual 1
        client.lPush(key, List(baz))() mustEqual 2
        client.lPop(key)() map (CBToString(_) mustEqual "baz")
        client.lPop(key)() map (CBToString(_) mustEqual "bar")
      }

      "push members and measure their length, then pop them off" in {
        val key = StringToChannelBuffer("llen")
        client.lLen(key)() mustEqual 0
        client.lPush(key, List(bar))() mustEqual 1
        client.lLen(key)() mustEqual 1
        client.lPush(key, List(baz))() mustEqual 2
        client.lLen(key)() mustEqual 2
        client.lPop(key)() map (CBToString(_) mustEqual "baz")
        client.lLen(key)() mustEqual 1
        client.lPop(key)() map (CBToString(_) mustEqual "bar")
        client.lLen(key)() mustEqual 0
      }

      "push members and index them, then pop them off, and index them" in {
        val key = StringToChannelBuffer("lindex")
        client.lIndex(key, 0)() mustEqual None
        client.lPush(key, List(bar))() mustEqual 1
        client.lIndex(key, 0)() map (CBToString(_) mustEqual "bar")
        client.lPush(key, List(baz))() mustEqual 2
        client.lIndex(key, 0)() map (CBToString(_) mustEqual "baz")
        client.lPop(key)() map (CBToString(_) mustEqual "baz")
        client.lIndex(key, 0)() map (CBToString(_) mustEqual "bar")
        client.lPop(key)() map (CBToString(_) mustEqual "bar")
      }

      "push a member, then insert some values, then pop them off" in {
        val key = StringToChannelBuffer("linsert")
        client.lPush(key, List(bar))() mustEqual 1
        client.lInsertAfter(key, bar, baz)
        client.lInsertBefore(key, bar, moo)
        client.lPop(key)() map (CBToString(_) mustEqual "moo")
        client.lPop(key)() map (CBToString(_) mustEqual "bar")
        client.lPop(key)() map (CBToString(_) mustEqual "baz")
      }

      "push members and remove one, then pop the other off" in {
        val key = StringToChannelBuffer("lremove")
        client.lPush(key, List(bar))() mustEqual 1
        client.lPush(key, List(baz))() mustEqual 2
        client.lRem(key, 1, baz)() mustEqual 1
        client.lPop(key)() map (CBToString(_) mustEqual "bar")
      }

      "push members and set one, then pop them off" in {
        val key = StringToChannelBuffer("lset")
        client.lPush(key, List(bar))() mustEqual 1
        client.lPush(key, List(baz))() mustEqual 2
        client.lSet(key, 0, moo)()
        client.lPop(key)() map (CBToString(_) mustEqual "moo")
        client.lPop(key)() map (CBToString(_) mustEqual "bar")
      }

      "push members examine the entire range, then pop them off" in {
        val key = StringToChannelBuffer("lrange")
        client.lPush(key, List(bar))() mustEqual 1
        client.lPush(key, List(baz))() mustEqual 2
        client.lRange(key, 0, -1)() map (CBToString(_)) mustEqual List("baz", "bar")
        client.lPop(key)() map (CBToString(_) mustEqual "baz")
        client.lPop(key)() map (CBToString(_) mustEqual "bar")
      }

      "push members, then pop them off of the other side, queue style" in {
        val key = StringToChannelBuffer("rpop")
        client.lPush(key, List(bar))() mustEqual 1
        client.lPush(key, List(baz))() mustEqual 2
        client.rPop(key)() map (CBToString(_) mustEqual "bar")
        client.rPop(key)() map (CBToString(_) mustEqual "baz")
      }

      "push members and then pop them off, except from the other side." in {
        val key = StringToChannelBuffer("rpop")
        client.rPush(key, List(bar))() mustEqual 1
        client.rPush(key, List(baz))() mustEqual 2
        client.rPop(key)() map (CBToString(_) mustEqual "baz")
        client.rPop(key)() map (CBToString(_) mustEqual "bar")
      }

      "push members, trimming as we go.  then pop off the two remaining." in {
        val key = StringToChannelBuffer("ltrim")
        client.lPush(key, List(bar))() mustEqual 1
        client.lPush(key, List(baz))() mustEqual 2
        client.lPush(key, List(boo))() mustEqual 3
        client.lTrim(key, 0, 1)()
        client.lPush(key, List(moo))() mustEqual 3
        client.lTrim(key, 0, 1)()
        client.rPop(key)() map (CBToString(_) mustEqual "boo")
        client.rPop(key)() map (CBToString(_) mustEqual "moo")
      }
    }

    "perform set commands" in {
      "add members to a set, then pop them off." in {
        val key = StringToChannelBuffer("pushpop")
        client.sAdd(key, List(bar))() mustEqual 1
        client.sAdd(key, List(baz))() mustEqual 1
        client.sPop(key)()
        client.sPop(key)()
      }

      "add members to a set, then pop them off, counting them." in {
        val key = StringToChannelBuffer("scard")
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
        val key = StringToChannelBuffer("members")
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
        val key = StringToChannelBuffer("members")
        client.sAdd(key, List(bar))() mustEqual 1
        client.sAdd(key, List(baz))() mustEqual 1
        val strings: CollectionSet[String] = (client.sMembers(key)() map (CBToString(_)))
        strings mustEqual CollectionSet("bar", "baz")
        client.sPop(key)()
        client.sPop(key)()
        client.sMembers(key)() mustEqual CollectionSet()
      }

      "add members to a set, then remove them." in {
        val key = StringToChannelBuffer("members")
        client.sAdd(key, List(bar))() mustEqual 1
        client.sAdd(key, List(baz))() mustEqual 1
        client.sRem(key, List(bar))() mustEqual 1
        client.sRem(key, List(baz))() mustEqual 1
        client.sRem(key, List(baz))() mustEqual 0
      }

    }

    "perform commands as a transaction" in {
      "set and get transaction" in {
        val txResult = client.transaction(Seq(Set(foo, bar), Set(baz, boo)))()
        ReplyFormat.toString(txResult.toList) mustEqual Seq("OK", "OK")
      }

      "hash set and multi get transaction" in {
        val txResult = client.transaction(Seq(HSet(foo, bar, baz), HSet(foo, boo, moo),
          HMGet(foo, Seq(bar, boo))))()
        ReplyFormat.toString(txResult.toList) mustEqual Seq("1", "1", "baz", "moo")
      }

      "key command on incorrect data type" in {
        val txResult = client.transaction(Seq(HSet(foo, boo, moo),
          Get(foo), HDel(foo, Seq(boo))))()
        txResult.toList mustEqual Seq(IntegerReply(1),
          ErrorReply("ERR Operation against a key holding the wrong kind of value"),
          IntegerReply(1))
      }

      "fail after a watched key is modified" in {
        client.set(foo, bar)()
        client.watch(Seq(foo))()
        client.set(foo, boo)()
        client.transaction(Seq(Get(foo)))() must throwA[ClientError]
      }

      "watch then unwatch a key" in {
        client.set(foo, bar)()
        client.watch(Seq(foo))()
        client.set(foo, boo)()
        client.unwatch()()
        val txResult = client.transaction(Seq(Get(foo)))()
        ReplyFormat.toString(txResult.toList) mustEqual Seq("boo")
      }

      "set followed by get on the same key" in {
        val txResult = client.transaction(Seq(Set(foo, bar), Get(foo)))()
        ReplyFormat.toString(txResult.toList) mustEqual Seq("OK", "bar")
      }

    }

  }

}
