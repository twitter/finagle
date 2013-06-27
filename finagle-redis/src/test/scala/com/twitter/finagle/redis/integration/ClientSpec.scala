package com.twitter.finagle.redis.integration

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.redis.protocol.{Limit, _}
import com.twitter.finagle.redis.util.{CBToString, RedisCluster, ReplyFormat, StringToChannelBuffer}
import com.twitter.finagle.redis.{ClientError, Redis, TransactionalClient}
import com.twitter.util.Await
import org.junit.Ignore
import org.specs.SpecificationWithJUnit
import scala.collection.{Set => CollectionSet}


// TODO(John Sirois): Convert these tests to run conditionally when an env-var is present at the
// least to get CI coverage.
@Ignore("These are ignored in the pom")
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
    val num = StringToChannelBuffer("num")

    doBefore {
      RedisCluster.start()
      client = TransactionalClient(
        ClientBuilder()
         .codec(new Redis())
         .hosts(RedisCluster.hostAddresses())
         .hostConnectionLimit(1)
         .buildFactory())
      Await.result(client.flushDB())
    }

    doAfter {
      client.release
      RedisCluster.stop()
    }

    "perform simple commands" in {
      "append" in {
        Await.result(client.set(foo, bar))
        Await.result(client.append(foo, baz)) mustEqual 6
      }

      "decrBy" in {
        Await.result(client.set(foo, StringToChannelBuffer("21")))
        Await.result(client.decrBy(foo, 2)) mustEqual 19
      }

      "del" in {
        Await.result(client.set(foo, bar))
        Await.result(client.del(Seq(foo)))
        Await.result(client.get(foo)) mustEqual None
      }

      "exists" in {
        Await.result(client.set(foo, bar))
        Await.result(client.exists(foo)) mustEqual true
      }

      "get range" in {
        Await.result(client.set(foo, StringToChannelBuffer("boing")))
        CBToString(Await.result(client.getRange(foo, 0, 2)).get) mustEqual "boi"
      }

      "set & get" in {
        Await.result(client.get(foo)) mustEqual None
        Await.result(client.set(foo, bar))
        CBToString(Await.result(client.get(foo)).get) mustEqual "bar"
      }

      "flush" in {
        Await.result(client.set(foo, bar))
        Await.result(client.flushDB())
        Await.result(client.get(foo)) mustEqual None
      }

      "select" in {
        Await.result(client.select(1)) mustEqual ()
      }

      "info" in {
        val infoCB = Await.result(client.info())
        val info = new String(infoCB.get.array, "UTF8")
        info mustMatch "# Server"
        info mustMatch "redis_version:"
        info mustMatch "# Clients"

        val cpuCB = Await.result(client.info(StringToChannelBuffer("cpu")))
        val cpu = new String(cpuCB.get.array, "UTF8")
        cpu mustMatch "# CPU"
        cpu mustMatch "used_cpu_sys:"
        cpu mustNotMatch "redis_version:"
      }

      "quit" in {
        Await.result(client.quit()) mustEqual ()
      }

      "ttl" in {
        Await.result(client.set(foo, bar))
        Await.result(client.expire(foo, 20)) mustEqual true
        Await.result(client.ttl(foo)) map (_ must beLessThanOrEqualTo(20L))
      }

      "expireAt" in {
        Await.result(client.set(foo, bar))
        val ttl = System.currentTimeMillis() + 20000L
        Await.result(client.expireAt(foo, ttl)) mustEqual true
        Await.result(client.ttl(foo)) map (_ must beLessThanOrEqualTo(ttl))
      }

      "pExpire(At) & pTtl" in {
        Await.result(client.set(foo, bar))
        Await.result(client.pExpire(foo, 100000L)) mustEqual true
        Await.result(client.pTtl(foo)) map (_ must beLessThanOrEqualTo(100000L))

        val ttl = System.currentTimeMillis() + 20000L
        Await.result(client.pExpireAt(foo, ttl)) mustEqual true
        Await.result(client.pTtl(foo)) map (_ must beLessThanOrEqualTo(20000L))
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
        Await.result(client.bitCount(foo)) mustEqual 0L
        Await.result(client.getBit(foo, 0)) mustEqual 0L
        Await.result(client.setBit(foo, 0, 1)) mustEqual 0L
        Await.result(client.getBit(foo, 0)) mustEqual 1L
        Await.result(client.setBit(foo, 0, 0)) mustEqual 1L

        Await.result(client.setBit(foo, 2, 1)) mustEqual 0L
        Await.result(client.setBit(foo, 3, 1)) mustEqual 0L

        Await.result(client.setBit(foo, 8, 1)) mustEqual 0L
        Await.result(client.bitCount(foo)) mustEqual 3L
        Await.result(client.bitCount(foo, Some(0), Some(0))) mustEqual 2L
        Await.result(client.setBit(foo, 8, 0)) mustEqual 1L

        Await.result(client.setBit(bar, 0, 1)) mustEqual 0L
        Await.result(client.setBit(bar, 3, 1)) mustEqual 0L

        Await.result(client.bitOp(BitOp.And, baz, Seq(foo, bar))) mustEqual 2L
        Await.result(client.bitCount(baz)) mustEqual 1L
        Await.result(client.getBit(baz, 0)) mustEqual 0L
        Await.result(client.getBit(baz, 3)) mustEqual 1L

        Await.result(client.bitOp(BitOp.Or, baz, Seq(foo, bar))) mustEqual 2L
        Await.result(client.bitCount(baz)) mustEqual 3L
        Await.result(client.getBit(baz, 0)) mustEqual 1L
        Await.result(client.getBit(baz, 1)) mustEqual 0L

        Await.result(client.bitOp(BitOp.Xor, baz, Seq(foo, bar))) mustEqual 2L
        Await.result(client.bitCount(baz)) mustEqual 2L
        Await.result(client.getBit(baz, 0)) mustEqual 1L
        Await.result(client.getBit(baz, 1)) mustEqual 0L

        Await.result(client.bitOp(BitOp.Not, baz, Seq(foo))) mustEqual 2L
        Await.result(client.bitCount(baz)) mustEqual 14
        Await.result(client.getBit(baz, 0)) mustEqual 1
        Await.result(client.getBit(baz, 2)) mustEqual 0
        Await.result(client.getBit(baz, 4)) mustEqual 1
      }

      "getSet" in {
        Await.result(client.getSet(foo, bar)) mustEqual None
        Await.result(client.get(foo)) mustEqual Some(bar)
        Await.result(client.getSet(foo, baz)) mustEqual Some(bar)
        Await.result(client.get(foo)) mustEqual Some(baz)
      }

      "incr / incrBy" in {
        Await.result(client.incr(foo)) mustEqual 1L
        Await.result(client.incrBy(foo, 10L)) mustEqual 11L
        Await.result(client.incrBy(bar, 10L)) mustEqual 10L
        Await.result(client.incr(bar)) mustEqual 11L
      }

      "mGet / mSet / mSetNx" in {
        Await.result(client.mSet(Map(foo -> bar, bar -> baz)))
        Await.result(client.mGet(Seq(foo, bar, baz))) mustEqual Seq(Some(bar), Some(baz), None)
        Await.result(client.mSetNx(Map(foo -> bar, baz -> foo, boo -> moo))) mustEqual false
        Await.result(client.mSetNx(Map(baz -> foo, boo -> moo))) mustEqual true
        Await.result(client.mGet(Seq(baz, boo))) mustEqual Seq(Some(foo), Some(moo))
      }

      "set variations" in {
        Await.result(client.pSetEx(foo, 10000L, bar))
        Await.result(client.get(foo)) mustEqual Some(bar)
        Await.result(client.ttl(foo)) map (_ must beLessThanOrEqualTo(10L))

        Await.result(client.setEx(bar, 10L, foo))
        Await.result(client.get(bar)) mustEqual Some(foo)
        Await.result(client.ttl(bar)) map (_ must beLessThanOrEqualTo(10L))

        Await.result(client.setNx(baz, foo)) mustEqual true
        Await.result(client.setNx(baz, bar)) mustEqual false

        Await.result(client.setRange(baz, 1, baz)) mustEqual 4L
        Await.result(client.get(baz)) mustEqual Some(StringToChannelBuffer("fbaz"))
      }

      "new set syntax variations" in {
        Await.result(client.setExNx(foo, 10L, bar)) mustEqual true
        Await.result(client.get(foo)) mustEqual Some(bar)
        Await.result(client.ttl(foo)) map (_ must beLessThanOrEqualTo(10L))
        Await.result(client.setExNx(foo, 10L, baz)) mustEqual false

        Await.result(client.setPxNx(bar, 10000L, baz)) mustEqual true
        Await.result(client.get(bar)) mustEqual Some(baz)
        Await.result(client.ttl(bar)) map (_ must beLessThanOrEqualTo(10L))
        Await.result(client.setPxNx(bar, 100L, bar)) mustEqual false

        Await.result(client.setXx(baz, foo)) mustEqual false
        Await.result(client.set(baz, foo))
        Await.result(client.setXx(baz, bar)) mustEqual true
        Await.result(client.get(baz)) mustEqual Some(bar)

        Await.result(client.setExXx(boo, 10L, foo)) mustEqual false
        Await.result(client.set(boo, foo))
        Await.result(client.setExXx(boo, 10L, bar)) mustEqual true
        Await.result(client.get(boo)) mustEqual Some(bar)
        Await.result(client.ttl(boo)) map (_ must beLessThanOrEqualTo(10L))

        Await.result(client.setPxXx(moo, 10000L, foo)) mustEqual false
        Await.result(client.set(moo, foo))
        Await.result(client.setPxXx(moo, 10000L, bar)) mustEqual true
        Await.result(client.get(moo)) mustEqual Some(bar)
        Await.result(client.ttl(moo)) map (_ must beLessThanOrEqualTo(10L))

        Await.result(client.setPx(num, 10000L, foo))
        Await.result(client.get(num)) mustEqual Some(foo)
        Await.result(client.ttl(num)) map (_ must beLessThanOrEqualTo(10L))
      }

      "strlen" in {
        Await.result(client.strlen(foo)) mustEqual 0L
        Await.result(client.set(foo, bar))
        Await.result(client.strlen(foo)) mustEqual 3L
      }
    }

    "perform hash commands" in {

      "hash set and get" in {
        Await.result(client.hSet(foo, bar, baz))
        CBToString(Await.result(client.hGet(foo, bar)).get) mustEqual "baz"
        Await.result(client.hGet(foo, boo)) mustEqual None
        Await.result(client.hGet(bar, baz)) mustEqual None
      }

      "delete a single field" in {
        Await.result(client.hSet(foo, bar, baz))
        Await.result(client.hDel(foo, Seq(bar))) mustEqual 1
        Await.result(client.hDel(foo, Seq(baz))) mustEqual 0
      }

      "delete multiple fields" in {
        Await.result(client.hSet(foo, bar, baz))
        Await.result(client.hSet(foo, boo, moo))
        Await.result(client.hDel(foo, Seq(bar, boo))) mustEqual 2
      }

      "get multiple values" in {
        Await.result(client.hSet(foo, bar, baz))
        Await.result(client.hSet(foo, boo, moo))
        CBToString.fromList(
          Await.result(client.hMGet(foo, Seq(bar, boo))).toList) mustEqual Seq("baz", "moo")
      }

      "set multiple values" in {
        Await.result(client.hMSet(foo, Map(baz -> bar, moo -> boo)))
        CBToString.fromList(
          Await.result(client.hMGet(foo, Seq(baz, moo))).toList) mustEqual Seq("bar", "boo")
      }

      "get multiple values at once" in {
        Await.result(client.hSet(foo, bar, baz))
        Await.result(client.hSet(foo, boo, moo))
        CBToString.fromTuples(
          Await.result(client.hGetAll(foo))) mustEqual Seq(("bar", "baz"), ("boo", "moo"))
      }

      "increment a value" in {
        Await.result(client.hIncrBy(foo, num, 4L))
        Await.result(client.hGet(foo, num)) mustEqual Some(StringToChannelBuffer(4L.toString))
        Await.result(client.hIncrBy(foo, num, 4L))
        Await.result(client.hGet(foo, num)) mustEqual Some(StringToChannelBuffer(8L.toString))
      }

      "do a setnx" in {
        Await.result(client.hDel(foo, Seq(bar)))
        Await.result(client.hSetNx(foo,bar, baz)) must_== 1
        Await.result(client.hSetNx(foo,bar, moo)) must_== 0
        CBToString(Await.result(client.hGet(foo, bar)).get) mustEqual "baz"
      }

      "get all the values" in {
        Await.result(client.del(Seq(foo)))
        Await.result(client.hMSet(foo, Map(baz -> bar, moo -> boo)))
        Await.result(client.hVals(foo)).map(CBToString(_)) mustEqual Seq("bar", "boo")
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
        Await.result(client.zAdd(foo, 10.5, bar)) mustEqual 1
        Await.result(client.zAdd(foo, 20.1, baz)) mustEqual 1
        Await.result(client.zScore(foo, bar)).get mustEqual 10.5
        Await.result(client.zScore(foo, baz)).get mustEqual 20.1
      }

      "add multiple members and get scores" in {
        Await.result(client.zAddMulti(foo, Seq((10.5, bar), (20.1, baz)))) mustEqual 2
        Await.result(client.zScore(foo, bar)).get mustEqual 10.5
        Await.result(client.zScore(foo, baz)).get mustEqual 20.1
      }

      "add members and get the zcount" in {
        Await.result(client.zAdd(foo, 10, bar)) mustEqual 1
        Await.result(client.zAdd(foo, 20, baz)) mustEqual 1
        Await.result(client.zCount(foo, ZInterval(0), ZInterval(30))) mustEqual 2
        Await.result(client.zCount(foo, ZInterval(40), ZInterval(50))) mustEqual 0
      }

      "get the zRangeByScore" in {
        Await.result(client.zAdd(foo, 10, bar)) mustEqual 1
        Await.result(client.zAdd(foo, 20, baz)) mustEqual 1
        for (left <- Await.result(client.zRangeByScore(foo, ZInterval(0), ZInterval(30), true,
          Some(Limit(0, 5)))).left)
          CBToString.fromTuplesWithDoubles(left.asTuples) mustEqual (Seq(("bar", 10), ("baz", 20)))
        for (left <- Await.result(client.zRangeByScore(foo, ZInterval(30), ZInterval(0), true,
          Some(Limit(0, 5)))).left)
          left.asTuples mustEqual Seq()
      }

      "get cardinality and remove members" in {
        Await.result(client.zAdd(foo, 10, bar)) mustEqual 1
        Await.result(client.zAdd(foo, 20, baz)) mustEqual 1
        Await.result(client.zCard(foo)) mustEqual 2
        Await.result(client.zRem(foo, Seq(bar, baz))) mustEqual 2
      }

      "get zRevRange" in {
        Await.result(client.zAdd(foo, 10, bar)) mustEqual 1
        Await.result(client.zAdd(foo, 20, baz)) mustEqual 1
        for (right <- Await.result(client.zRevRange(foo, 0, -1, false)).right)
          CBToString.fromList(right.toList) mustEqual Seq("baz", "bar")
      }

      "get zRevRangeByScore" in {
        Await.result(client.zAdd(foo, 10, bar)) mustEqual 1
        Await.result(client.zAdd(foo, 20, baz)) mustEqual 1
        for (left <- Await.result(client.zRevRangeByScore(foo, ZInterval(10), ZInterval(0), true,
            Some(Limit(0, 1)))).left)
          CBToString.fromTuplesWithDoubles(left.asTuples) mustEqual Seq(("bar", 10))
        for (left <- Await.result(client.zRevRangeByScore(foo, ZInterval(0), ZInterval(10), true,
            Some(Limit(0, 1)))).left)
          left.asTuples mustEqual Seq()
        for (left <- Await.result(client.zRevRangeByScore(foo, ZInterval(0), ZInterval(0), true,
          Some(Limit(0, 1)))).left)
          left.asTuples mustEqual Seq()
      }

      "add members and zIncr, then zIncr a nonmember" in {
        Await.result(client.zAdd(foo, 10, bar)) mustEqual 1
        Await.result(client.zIncrBy(foo, 10, bar)) mustEqual Some(20)
        Await.result(client.zIncrBy(foo, 10, baz)) mustEqual Some(10)
      }

      "get zRange" in {
        Await.result(client.zAdd(foo, 10, bar)) mustEqual 1
        Await.result(client.zAdd(foo, 20, baz)) mustEqual 1
        Await.result(client.zAdd(foo, 30, boo)) mustEqual 1
        for (right <- Await.result(client.zRange(foo, 0, -1, false)).right)
          CBToString.fromList(right.toList) mustEqual List("bar", "baz", "boo")
        for (right <- Await.result(client.zRange(foo, 2, 3, false)).right)
          CBToString.fromList(right.toList) mustEqual List("boo")
        for (right <- Await.result(client.zRange(foo, -2, -1, false)).right)
          CBToString.fromList(right.toList) mustEqual List("baz", "boo")
      }

      "get zRank" in {
        Await.result(client.zAdd(foo, 10, bar)) mustEqual 1
        Await.result(client.zAdd(foo, 20, baz)) mustEqual 1
        Await.result(client.zAdd(foo, 30, boo)) mustEqual 1
        Await.result(client.zRank(foo, boo)) mustEqual Some(2)
        Await.result(client.zRank(foo, moo)) mustEqual None
      }

      "get zRemRangeByRank" in {
        Await.result(client.zAdd(foo, 10, bar)) mustEqual 1
        Await.result(client.zAdd(foo, 20, baz)) mustEqual 1
        Await.result(client.zAdd(foo, 30, boo)) mustEqual 1
        Await.result(client.zRemRangeByRank(foo, 0, 1)) mustEqual 2
        for (right <- Await.result(client.zRange(foo, 0, -1, false)).right)
          CBToString.fromList(right.toList) mustEqual List("boo")
      }

      "get zRemRangeByScore" in {
        Await.result(client.zAdd(foo, 10, bar)) mustEqual 1
        Await.result(client.zAdd(foo, 20, baz)) mustEqual 1
        Await.result(client.zAdd(foo, 30, boo)) mustEqual 1
        Await.result(client.zRemRangeByScore(foo, ZInterval(10), ZInterval(20))) mustEqual 2
        for (right <- Await.result(client.zRange(foo, 0, -1, false)).right)
          CBToString.fromList(right.toList) mustEqual List("boo")
      }

      "get zRevRank" in {
        Await.result(client.zAdd(foo, 10, bar)) mustEqual 1
        Await.result(client.zAdd(foo, 20, baz)) mustEqual 1
        Await.result(client.zAdd(foo, 30, boo)) mustEqual 1
        Await.result(client.zRevRank(foo, boo)) mustEqual Some(0)
        Await.result(client.zRevRank(foo, moo)) mustEqual None
      }

    }

    "perform list commands" in {
      "push members and pop them off" in {
        val key = StringToChannelBuffer("push")
        Await.result(client.lPush(key, List(bar))) mustEqual 1
        Await.result(client.lPush(key, List(baz))) mustEqual 2
        Await.result(client.lPop(key)) map (CBToString(_) mustEqual "baz")
        Await.result(client.lPop(key)) map (CBToString(_) mustEqual "bar")
      }

      "push members and measure their length, then pop them off" in {
        val key = StringToChannelBuffer("llen")
        Await.result(client.lLen(key)) mustEqual 0
        Await.result(client.lPush(key, List(bar))) mustEqual 1
        Await.result(client.lLen(key)) mustEqual 1
        Await.result(client.lPush(key, List(baz))) mustEqual 2
        Await.result(client.lLen(key)) mustEqual 2
        Await.result(client.lPop(key)) map (CBToString(_) mustEqual "baz")
        Await.result(client.lLen(key)) mustEqual 1
        Await.result(client.lPop(key)) map (CBToString(_) mustEqual "bar")
        Await.result(client.lLen(key)) mustEqual 0
      }

      "push members and index them, then pop them off, and index them" in {
        val key = StringToChannelBuffer("lindex")
        Await.result(client.lIndex(key, 0)) mustEqual None
        Await.result(client.lPush(key, List(bar))) mustEqual 1
        Await.result(client.lIndex(key, 0)) map (CBToString(_) mustEqual "bar")
        Await.result(client.lPush(key, List(baz))) mustEqual 2
        Await.result(client.lIndex(key, 0)) map (CBToString(_) mustEqual "baz")
        Await.result(client.lPop(key)) map (CBToString(_) mustEqual "baz")
        Await.result(client.lIndex(key, 0)) map (CBToString(_) mustEqual "bar")
        Await.result(client.lPop(key)) map (CBToString(_) mustEqual "bar")
      }

      "push a member, then insert some values, then pop them off" in {
        val key = StringToChannelBuffer("linsert")
        Await.result(client.lPush(key, List(bar))) mustEqual 1
        client.lInsertAfter(key, bar, baz)
        client.lInsertBefore(key, bar, moo)
        Await.result(client.lPop(key)) map (CBToString(_) mustEqual "moo")
        Await.result(client.lPop(key)) map (CBToString(_) mustEqual "bar")
        Await.result(client.lPop(key)) map (CBToString(_) mustEqual "baz")
      }

      "push members and remove one, then pop the other off" in {
        val key = StringToChannelBuffer("lremove")
        Await.result(client.lPush(key, List(bar))) mustEqual 1
        Await.result(client.lPush(key, List(baz))) mustEqual 2
        Await.result(client.lRem(key, 1, baz)) mustEqual 1
        Await.result(client.lPop(key)) map (CBToString(_) mustEqual "bar")
      }

      "push members and set one, then pop them off" in {
        val key = StringToChannelBuffer("lset")
        Await.result(client.lPush(key, List(bar))) mustEqual 1
        Await.result(client.lPush(key, List(baz))) mustEqual 2
        Await.result(client.lSet(key, 0, moo))
        Await.result(client.lPop(key)) map (CBToString(_) mustEqual "moo")
        Await.result(client.lPop(key)) map (CBToString(_) mustEqual "bar")
      }

      "push members examine the entire range, then pop them off" in {
        val key = StringToChannelBuffer("lrange")
        Await.result(client.lPush(key, List(bar))) mustEqual 1
        Await.result(client.lPush(key, List(baz))) mustEqual 2
        Await.result(client.lRange(key, 0, -1)) map (CBToString(_)) mustEqual List("baz", "bar")
        Await.result(client.lPop(key)) map (CBToString(_) mustEqual "baz")
        Await.result(client.lPop(key)) map (CBToString(_) mustEqual "bar")
      }

      "push members, then pop them off of the other side, queue style" in {
        val key = StringToChannelBuffer("rpop")
        Await.result(client.lPush(key, List(bar))) mustEqual 1
        Await.result(client.lPush(key, List(baz))) mustEqual 2
        Await.result(client.rPop(key)) map (CBToString(_) mustEqual "bar")
        Await.result(client.rPop(key)) map (CBToString(_) mustEqual "baz")
      }

      "push members and then pop them off, except from the other side." in {
        val key = StringToChannelBuffer("rpop")
        Await.result(client.rPush(key, List(bar))) mustEqual 1
        Await.result(client.rPush(key, List(baz))) mustEqual 2
        Await.result(client.rPop(key)) map (CBToString(_) mustEqual "baz")
        Await.result(client.rPop(key)) map (CBToString(_) mustEqual "bar")
      }

      "push members, trimming as we go.  then pop off the two remaining." in {
        val key = StringToChannelBuffer("ltrim")
        Await.result(client.lPush(key, List(bar))) mustEqual 1
        Await.result(client.lPush(key, List(baz))) mustEqual 2
        Await.result(client.lPush(key, List(boo))) mustEqual 3
        Await.result(client.lTrim(key, 0, 1))
        Await.result(client.lPush(key, List(moo))) mustEqual 3
        Await.result(client.lTrim(key, 0, 1))
        Await.result(client.rPop(key)) map (CBToString(_) mustEqual "boo")
        Await.result(client.rPop(key)) map (CBToString(_) mustEqual "moo")
      }
    }

    "perform set commands" in {
      "add members to a set, then pop them off." in {
        val key = StringToChannelBuffer("pushpop")
        Await.result(client.sAdd(key, List(bar))) mustEqual 1
        Await.result(client.sAdd(key, List(baz))) mustEqual 1
        Await.result(client.sPop(key))
        Await.result(client.sPop(key))
      }

      "add members to a set, then pop them off, counting them." in {
        val key = StringToChannelBuffer("scard")
        Await.result(client.sAdd(key, List(bar))) mustEqual 1
        Await.result(client.sCard(key)) mustEqual 1
        Await.result(client.sAdd(key, List(baz))) mustEqual 1
        Await.result(client.sCard(key)) mustEqual 2
        Await.result(client.sPop(key))
        Await.result(client.sCard(key)) mustEqual 1
        Await.result(client.sPop(key))
        Await.result(client.sCard(key)) mustEqual 0
      }

      "add members to a set, look for some, pop them off, look for some again." in {
        val key = StringToChannelBuffer("members")
        Await.result(client.sAdd(key, List(bar))) mustEqual 1
        Await.result(client.sIsMember(key, bar)) mustEqual true
        Await.result(client.sIsMember(key, baz)) mustEqual false
        Await.result(client.sAdd(key, List(baz))) mustEqual 1
        Await.result(client.sIsMember(key, bar)) mustEqual true
        Await.result(client.sIsMember(key, baz)) mustEqual true
        Await.result(client.sPop(key))
        Await.result(client.sPop(key))
        Await.result(client.sIsMember(key, bar)) mustEqual false
        Await.result(client.sIsMember(key, baz)) mustEqual false
      }

      "add members to a set, then examine them, then pop them off, then examien them again." in {
        val key = StringToChannelBuffer("members")
        Await.result(client.sAdd(key, List(bar))) mustEqual 1
        Await.result(client.sAdd(key, List(baz))) mustEqual 1
        val strings: CollectionSet[String] = (Await.result(client.sMembers(key)) map (CBToString(_)))
        strings mustEqual CollectionSet("bar", "baz")
        Await.result(client.sPop(key))
        Await.result(client.sPop(key))
        Await.result(client.sMembers(key)) mustEqual CollectionSet()
      }

      "add members to a set, then remove them." in {
        val key = StringToChannelBuffer("members")
        Await.result(client.sAdd(key, List(bar))) mustEqual 1
        Await.result(client.sAdd(key, List(baz))) mustEqual 1
        Await.result(client.sRem(key, List(bar))) mustEqual 1
        Await.result(client.sRem(key, List(baz))) mustEqual 1
        Await.result(client.sRem(key, List(baz))) mustEqual 0
      }

    }

    "perform commands as a transaction" in {
      "set and get transaction" in {
        val txResult = Await.result(client.transaction(Seq(Set(foo, bar), Set(baz, boo))))
        ReplyFormat.toString(txResult.toList) mustEqual Seq("OK", "OK")
      }

      "hash set and multi get transaction" in {
        val txResult = Await.result(client.transaction(Seq(HSet(foo, bar, baz), HSet(foo, boo, moo),
          HMGet(foo, Seq(bar, boo)))))
        ReplyFormat.toString(txResult.toList) mustEqual Seq("1", "1", "baz", "moo")
      }

      "key command on incorrect data type" in {
        val txResult = Await.result(client.transaction(Seq(HSet(foo, boo, moo),
          Get(foo), HDel(foo, Seq(boo)))))
        txResult.toList mustEqual Seq(IntegerReply(1),
          ErrorReply("ERR Operation against a key holding the wrong kind of value"),
          IntegerReply(1))
      }

      "fail after a watched key is modified" in {
        Await.result(client.set(foo, bar))
        Await.result(client.watch(Seq(foo)))
        Await.result(client.set(foo, boo))
        Await.result(client.transaction(Seq(Get(foo)))) must throwA[ClientError]
      }

      "watch then unwatch a key" in {
        Await.result(client.set(foo, bar))
        Await.result(client.watch(Seq(foo)))
        Await.result(client.set(foo, boo))
        Await.result(client.unwatch())
        val txResult = Await.result(client.transaction(Seq(Get(foo))))
        ReplyFormat.toString(txResult.toList) mustEqual Seq("boo")
      }

      "set followed by get on the same key" in {
        val txResult = Await.result(client.transaction(Seq(Set(foo, bar), Get(foo))))
        ReplyFormat.toString(txResult.toList) mustEqual Seq("OK", "bar")
      }

    }

  }

}
