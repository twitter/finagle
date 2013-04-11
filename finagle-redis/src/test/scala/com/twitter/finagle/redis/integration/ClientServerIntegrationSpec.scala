package com.twitter.finagle.redis.integration

import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.redis._
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.Service
import com.twitter.util.Future
import java.net.InetSocketAddress
import org.jboss.netty.buffer.ChannelBuffer
import org.specs.SpecificationWithJUnit
import util._
import scala.collection.immutable

class ClientServerIntegrationSpec extends SpecificationWithJUnit {
  lazy val svcClient = ClientBuilder()
    .name("redis-client")
    .codec(Redis())
    .hosts(RedisCluster.hostAddresses())
    .hostConnectionLimit(2)
    .retries(2)
    .build()

  implicit def s2b(s: String) = StringToChannelBuffer(s)
  val service = new Service[Command, Reply] {
    def apply(cmd: Command): Future[Reply] = {
      svcClient(cmd)
    }
  }

  val server = ServerBuilder()
    .name("redis-server")
    .codec(Redis())
    .bindTo(new InetSocketAddress(0))
    .build(service)


  lazy val client = ClientBuilder()
    .name("redis-client")
    .codec(Redis())
    .hosts(server.localAddress)
    .hostConnectionLimit(2)
    .retries(2)
    .build()


  val KEY = StringToChannelBuffer("foo")
  val VALUE = StringToChannelBuffer("bar")

  doBeforeSpec {
    RedisCluster.start(1)
    client(Del(List(KEY)))()
    client(Set(KEY, VALUE))() mustEqual StatusReply("OK")
  }

  "A client" should {
    "Handle Key Commands" >> {
      "DEL" >> {
        client(Set("key1", "val1"))() mustEqual StatusReply("OK")
        client(Set("key2", "val2"))() mustEqual StatusReply("OK")
        client(Del(List(StringToChannelBuffer("key1"),
        StringToChannelBuffer("key2"))))() mustEqual IntegerReply(2)
        client(Del(null:List[ChannelBuffer]))() must throwA[ClientError]
        client(Del(List[ChannelBuffer]()))() must throwA[ClientError]
      }
      "EXISTS" >> {
        client(Exists(KEY))() mustEqual IntegerReply(1)
        client(Exists("nosuchkey"))() mustEqual IntegerReply(0)
        client(Exists(null:ChannelBuffer))() must throwA[ClientError]
      }
      "EXPIRE" >> {
        client(Expire("key1", 30))() mustEqual IntegerReply(0)
        client(Expire(null:ChannelBuffer, 30))() must throwA[ClientError]
        client(Expire(KEY, 3600))() mustEqual IntegerReply(1)
        assertIntegerReply(client(Ttl(KEY)), 3600)
      }
      "EXPIREAT" >> {
        import com.twitter.util.Time
        import com.twitter.conversions.time._
        val t = Time.now + 3600.seconds
        client(ExpireAt("key1", t))() mustEqual IntegerReply(0)
        client(ExpireAt(KEY, t))() mustEqual IntegerReply(1)
        assertIntegerReply(client(Ttl(KEY)), 3600)
        client(ExpireAt(null, t))() must throwA[ClientError]
      }
      "KEYS" >> {
         val request = client(Keys("*"))
         val expects = List("foo")
         assertMBulkReply(request, expects, true)
      }
      "PERSIST" >> {
        client(Persist("nosuchkey"))() mustEqual IntegerReply(0)
        client(Set("persist", "value"))()
        client(Persist("persist"))() mustEqual IntegerReply(0)
        client(Expire("persist", 30))()
        client(Persist("persist"))() mustEqual IntegerReply(1)
        client(Ttl("persist"))() mustEqual IntegerReply(-1)
        client(Del(List(StringToChannelBuffer("persist"))))()
      }
      "RENAME" >> {
        client(Set("rename1", "foo"))()
        assertBulkReply(client(Get("rename1")), "foo")
        client(Rename("rename1", "rename1"))() must haveClass[ErrorReply]
        client(Rename("nosuchkey", "foo"))() must haveClass[ErrorReply]
        client(Rename("rename1", "rename2"))() mustEqual StatusReply("OK")
        assertBulkReply(client(Get("rename2")), "foo")
      }
      "RENAMENX" >> {
        client(Set("renamenx1", "foo"))()
        assertBulkReply(client(Get("renamenx1")), "foo")
        client(RenameNx("renamenx1", "renamenx1"))() must haveClass[ErrorReply]
        client(RenameNx("nosuchkey", "foo"))() must haveClass[ErrorReply]
        client(RenameNx("renamenx1", "renamenx2"))() mustEqual IntegerReply(1)
        assertBulkReply(client(Get("renamenx2")), "foo")
        client(RenameNx("renamenx2", KEY))() mustEqual IntegerReply(0)
      }
      "RANDOMKEY" >> {
        client(Randomkey())() must haveClass[BulkReply]
      }
      "TTL" >> { // tested by expire/expireat
        client(Ttl(""))() must throwA[ClientError]
        client(Ttl("thing"))() mustEqual IntegerReply(-1)
      }
      "TYPE" >> {
        client(Type(KEY))() mustEqual StatusReply("string")
        client(Type("nosuchkey"))() mustEqual StatusReply("none")
      }
    }

    "Handle Sorted Set Commands" >> {
      val ZKEY = StringToChannelBuffer("zkey")
      val ZVAL = List(ZMember(1, "one"), ZMember(2, "two"), ZMember(3, "three"))

      def zAdd(key: String, members: ZMember*) {
        members.foreach { member =>
          client(ZAdd(key, List(member)))() mustEqual IntegerReply(1)
        }
      }

      doBefore {
        ZVAL.foreach { zv =>
          client(ZAdd(ZKEY, List(zv)))() mustEqual IntegerReply(1)
        }
      }
      doAfter {
        client(Del(List(ZKEY)))() mustEqual IntegerReply(1)
      }

      "ZADD" >> {
        client(ZAdd("zadd1", List(ZMember(1, "one"))))() mustEqual IntegerReply(1)
        client(ZAdd("zadd1", List(ZMember(2, "two"))))() mustEqual IntegerReply(1)
        client(ZAdd("zadd1", List(ZMember(3, "two"))))() mustEqual IntegerReply(0)
        val expected = List("one", "1", "two", "3")
        assertMBulkReply(client(ZRange("zadd1", 0, -1, WithScores)), expected)
        assertMBulkReply(client(ZRange("zadd1", 0, -1)), List("one", "two"))
      }
      "ZCARD" >> {
        client(ZCard(ZKEY))() mustEqual IntegerReply(3)
        client(ZCard("nosuchkey"))() mustEqual IntegerReply(0)
        client(ZCard(KEY))() must haveClass[ErrorReply]
      }
      "ZCOUNT" >> {
        client(ZCount(ZKEY, ZInterval.MIN, ZInterval.MAX))() mustEqual IntegerReply(3)
        client(ZCount(ZKEY, ZInterval.exclusive(1), ZInterval(3)))() mustEqual IntegerReply(2)
      }
      "ZINCRBY" >> {
        zAdd("zincrby1", ZMember(1, "one"), ZMember(2, "two"))
        assertBulkReply(client(ZIncrBy("zincrby1", 2, "one")), "3")
        assertMBulkReply(
          client(ZRange("zincrby1", 0, -1, WithScores)),
          List("two", "2", "one", "3"))
      }
      "ZINTERSTORE/ZUNIONSTORE" >> {
        val key = "zstore1"
        val key2 = "zstore2"
        zAdd(key, ZMember(1, "one"), ZMember(2, "two"))
        zAdd(key2, ZMember(1, "one"), ZMember(2, "two"), ZMember(3, "three"))

        client(ZInterStore("out", List(key, key2), Weights(2,3)))() mustEqual IntegerReply(2)
        assertMBulkReply(
          client(ZRange("out", 0, -1, WithScores)),
          List("one", "5", "two", "10"))

        client(ZUnionStore("out", List(key, key2), Weights(2,3)))() mustEqual IntegerReply(3)
        assertMBulkReply(
          client(ZRange("out", 0, -1, WithScores)),
          List("one", "5", "three", "9", "two", "10"))
      }
      "ZRANGE/ZREVRANGE" >> {
        zAdd("zrange1", ZMember(1, "one"), ZMember(2, "two"), ZMember(3, "three"))

        assertMBulkReply(client(ZRange("zrange1", 0, -1)), List("one", "two", "three"))
        assertMBulkReply(
          client(ZRange("zrange1", 0, -1, WithScores)),
          List("one", "1", "two", "2", "three", "3"))
        assertMBulkReply(client(ZRange("zrange1", 2, 3)), List("three"))
        assertMBulkReply(
          client(ZRange("zrange1", 2, 3, WithScores)),
          List("three", "3"))
        assertMBulkReply(client(ZRange("zrange1", -2, -1)), List("two", "three"))
        assertMBulkReply(
          client(ZRange("zrange1", -2, -1, WithScores)),
          List("two", "2", "three", "3"))

        assertMBulkReply(
          client(ZRevRange("zrange1", 0, -1)),
          List("three", "two", "one"))
        assertMBulkReply(
          client(ZRevRange("zrange1", 2, 3)),
          List("one"))
        assertMBulkReply(
          client(ZRevRange("zrange1", -2, -1)),
          List("two", "one"))
      }
      "ZRANGEBYSCORE/ZREVRANGEBYSCORE" >> {
        val key = "zrangebyscore1"
        zAdd(key, ZMember(1, "one"), ZMember(2, "two"), ZMember(3, "three"))
        assertMBulkReply(
          client(ZRangeByScore(key, ZInterval.MIN, ZInterval.MAX)),
          List("one", "two", "three"))
        assertMBulkReply(
          client(ZRangeByScore(key, ZInterval(1f), ZInterval(2f))),
          List("one", "two"))
        assertMBulkReply(
          client(ZRangeByScore(key, ZInterval.exclusive(1f), ZInterval(2f))),
          List("two"))
        assertMBulkReply(
          client(ZRangeByScore(key, ZInterval.exclusive(1f), ZInterval.exclusive(2f))),
          List())
        assertMBulkReply(
          client(ZRangeByScore(key, ZInterval.MIN, ZInterval.MAX, Limit(1,5))),
          List("two","three"))

        assertMBulkReply(
          client(ZRevRangeByScore(key, ZInterval.MAX, ZInterval.MIN)),
          List("three", "two", "one"))
        assertMBulkReply(
          client(ZRevRangeByScore(key, ZInterval(2f), ZInterval(1f))),
          List("two", "one"))
        assertMBulkReply(
          client(ZRevRangeByScore(key, ZInterval(2f), ZInterval.exclusive(1f))),
          List("two"))
        assertMBulkReply(
          client(ZRevRangeByScore(key, ZInterval.exclusive(2f), ZInterval.exclusive(1f))),
          List())
      }
      "ZRANK/ZREVRANK" >> {
        val key = "zrank1"
        zAdd(key, ZMember(1, "one"), ZMember(2, "two"), ZMember(3, "three"))
        client(ZRank(key, "three"))() mustEqual IntegerReply(2)
        client(ZRank(key, "four"))() mustEqual EmptyBulkReply()
        client(ZRevRank(key, "one"))() mustEqual IntegerReply(2)
        client(ZRevRank(key, "four"))() mustEqual EmptyBulkReply()
      }
      "ZREM" >> {
        val key = "zrem1"
        zAdd(key, ZMember(1, "one"), ZMember(2, "two"), ZMember(3, "three"))
        client(ZRem(key, List("two")))() mustEqual IntegerReply(1)
        client(ZRem(key, List("nosuchmember")))() mustEqual IntegerReply(0)
        assertMBulkReply(
          client(ZRange(key, 0, -1, WithScores)),
          List("one", "1", "three", "3"))
      }
      "ZREMRANGEBYRANK" >> {
        val key = "zremrangebyrank1"
        zAdd(key, ZMember(1, "one"), ZMember(2, "two"), ZMember(3, "three"))
        client(ZRemRangeByRank(key, 0, 1))() mustEqual IntegerReply(2)
        assertMBulkReply(
          client(ZRange(key, 0, -1, WithScores)),
          List("three", "3"))
      }
      "ZREMRANGEBYSCORE" >> {
        val key = "zremrangebyscore1"
        zAdd(key, ZMember(1, "one"), ZMember(2, "two"), ZMember(3, "three"))
        client(ZRemRangeByScore(key, ZInterval.MIN, ZInterval.exclusive(2)))() mustEqual IntegerReply(1)
        assertMBulkReply(
          client(ZRange(key, 0, -1, WithScores)),
          List("two", "2", "three", "3"))
      }
      "ZSCORE" >> {
        zAdd("zscore1", ZMember(1, "one"))
        assertBulkReply(client(ZScore("zscore1", "one")), "1")
      }
    }

    "Handle String Commands" >> {
      "APPEND" >> {
        client(Append("append1", "Hello"))() mustEqual IntegerReply(5)
        client(Append("append1", " World"))() mustEqual IntegerReply(11)
        assertBulkReply(client(Get("append1")), "Hello World")
      }
      "BITCOUNT" >> {
        client(BitCount("bitcount"))() mustEqual IntegerReply(0L)
        client(Set("bitcount", "bar"))() mustEqual StatusReply("OK")
        client(BitCount("bitcount"))() mustEqual IntegerReply(10L)
        client(BitCount("bitcount", Some(2), Some(4)))() mustEqual IntegerReply(4L)
      }
      "BITOP" >> {
        client(SetBit("bitop1", 0, 1))() mustEqual IntegerReply(0L)
        client(SetBit("bitop1", 3, 1))() mustEqual IntegerReply(0L)
        client(SetBit("bitop2", 2, 1))() mustEqual IntegerReply(0L)
        client(SetBit("bitop2", 3, 1))() mustEqual IntegerReply(0L)

        client(BitOp(BitOp.And, "bitop3", Seq("bitop1", "bitop2")))() mustEqual IntegerReply(1L)
        client(GetBit("bitop3", 0))() mustEqual IntegerReply(0L)
        client(GetBit("bitop3", 3))() mustEqual IntegerReply(1L)

        client(BitOp(BitOp.Or, "bitop3", Seq("bitop1", "bitop2")))() mustEqual IntegerReply(1L)
        client(GetBit("bitop3", 0))() mustEqual IntegerReply(1L)
        client(GetBit("bitop3", 1))() mustEqual IntegerReply(0L)

        client(BitOp(BitOp.Xor, "bitop3", Seq("bitop1", "bitop2")))() mustEqual IntegerReply(1L)
        client(GetBit("bitop3", 0))() mustEqual IntegerReply(1L)
        client(GetBit("bitop3", 1))() mustEqual IntegerReply(0L)

        client(BitOp(BitOp.Not, "bitop3", Seq("bitop1")))() mustEqual IntegerReply(1L)
        client(GetBit("bitop3", 0))() mustEqual IntegerReply(0L)
        client(GetBit("bitop3", 1))() mustEqual IntegerReply(1L)
        client(GetBit("bitop3", 4))() mustEqual IntegerReply(1L)
      }
      "DECR" >> {
        client(Decr("decr1"))() mustEqual IntegerReply(-1)
        client(Decr("decr1"))() mustEqual IntegerReply(-2)
        client(Decr(KEY))() must haveClass[ErrorReply]
      }
      "DECRBY" >> {
        client(DecrBy("decrby1", 1))() mustEqual IntegerReply(-1)
        client(DecrBy("decrby1", 10))() mustEqual IntegerReply(-11)
        client(DecrBy(KEY, 1))() must haveClass[ErrorReply]
      }
      "GET" >> {
        client(Get("thing"))() must haveClass[EmptyBulkReply]
        assertBulkReply(client(Get(KEY)), "bar")
        client(Get(null:ChannelBuffer))() must throwA[ClientError]
        client(Get(null:List[Array[Byte]]))() must throwA[ClientError]
      }
      "GETBIT" >> {
        client(SetBit("getbit", 7, 1))() mustEqual IntegerReply(0)
        client(GetBit("getbit", 0))() mustEqual IntegerReply(0)
        client(GetBit("getbit", 7))() mustEqual IntegerReply(1)
        client(GetBit("getbit", 100))() mustEqual IntegerReply(0)
      }
      "GETRANGE" >> {
        val key = "getrange"
        val value = "This is a string"
        client(Set(key, value))() mustEqual StatusReply("OK")
        assertBulkReply(client(GetRange(key, 0, 3)), "This")
        assertBulkReply(client(GetRange(key, -3, -1)), "ing")
        assertBulkReply(client(GetRange(key, 0, -1)), value)
        assertBulkReply(client(GetRange(key, 10, 100)), "string")
      }
      "GETSET" >> {
        val key = "getset"
        client(Incr(key))() mustEqual IntegerReply(1)
        assertBulkReply(client(GetSet(key, "0")), "1")
        assertBulkReply(client(Get(key)), "0")
        client(GetSet("brandnewkey", "foo"))() mustEqual EmptyBulkReply()
      }
      "INCR" >> {
        client(Incr("incr1"))() mustEqual IntegerReply(1)
        client(Incr("incr1"))() mustEqual IntegerReply(2)
        client(Incr(KEY))() must haveClass[ErrorReply]
      }
      "INCRBY" >> {
        client(IncrBy("incrby1", 1))() mustEqual IntegerReply(1)
        client(IncrBy("incrby1", 10))() mustEqual IntegerReply(11)
        client(IncrBy(KEY, 1))() must haveClass[ErrorReply]
      }
      "MGET" >> {
        val expects = List(
          BytesToString(RedisCodec.NIL_VALUE_BA.array),
          BytesToString(VALUE.array)
        )
        val req = client(MGet(List(StringToChannelBuffer("thing"), KEY)))
        assertMBulkReply(req, expects)
        client(MGet(List[ChannelBuffer]()))() must throwA[ClientError]
      }
      "MSET" >> {
        val input = Map(
          StringToChannelBuffer("thing")   -> StringToChannelBuffer("thang"),
          KEY       -> VALUE,
          StringToChannelBuffer("stuff")   -> StringToChannelBuffer("bleh")
          )
        client(MSet(input))() mustEqual StatusReply("OK")
        val req = client(MGet(List(StringToChannelBuffer("thing"), KEY,
          StringToChannelBuffer("noexists"),
          StringToChannelBuffer("stuff"))))
        val expects = List("thang", "bar", BytesToString(RedisCodec.NIL_VALUE_BA.array), "bleh")
        assertMBulkReply(req, expects)
      }
      "MSETNX" >> {
        val input1 = Map(
          StringToChannelBuffer("msnx.key1") -> s2b("Hello"),
          StringToChannelBuffer("msnx.key2") -> s2b("there")
        )
        client(MSetNx(input1))() mustEqual IntegerReply(1)
        val input2 = Map(
          StringToChannelBuffer("msnx.key2") -> s2b("there"),
          StringToChannelBuffer("msnx.key3") -> s2b("world")
        )
        client(MSetNx(input2))() mustEqual IntegerReply(0)
        val expects = List("Hello", "there", BytesToString(RedisCodec.NIL_VALUE_BA.array))
        assertMBulkReply(client(MGet(List(StringToChannelBuffer("msnx.key1"),
          StringToChannelBuffer("msnx.key2"),
          StringToChannelBuffer("msnx.key3")))), expects)
      }
      "PSETEX" >> {
        client(PSetEx(null, 300000L, "value"))() must throwA[ClientError]
        client(PSetEx("psetex1", 300000L, null))() must throwA[ClientError]
        client(PSetEx("psetex1", 0L, "value"))() must throwA[ClientError]
        client(PSetEx("psetex1", 300000L, "value"))() mustEqual StatusReply("OK")
      }
      "SET" >> {
        client(Set(null, null))() must throwA[ClientError]
        client(Set("key1", null))() must throwA[ClientError]
        client(Set(null, "value1"))() must throwA[ClientError]
      }
      "SETBIT" >> {
        client(SetBit("setbit", 7, 1))() mustEqual IntegerReply(0)
        client(SetBit("setbit", 7, 0))() mustEqual IntegerReply(1)
        assertBulkReply(client(Get("setbit")), BytesToString(Array[Byte](0)))
      }
      "SETEX" >> {
        val key = "setex"
        client(SetEx(key, 10, "Hello"))() mustEqual StatusReply("OK")
        client(Ttl(key))() match {
          case IntegerReply(seconds) => seconds.toInt must beCloseTo(10, 2)
          case _ => fail("Expected IntegerReply")
        }
        assertBulkReply(client(Get(key)), "Hello")
      }
      "SETNX" >> {
        val key = "setnx"
        val value1 = "Hello"
        val value2 = "World"
        client(SetNx(key, value1))() mustEqual IntegerReply(1)
        client(SetNx(key, value2))() mustEqual IntegerReply(0)
        assertBulkReply(client(Get(key)), value1)
      }
      "SETRANGE" >> {
        val key = "setrange"
        val value = "Hello World"
        client(Set(key, value))() mustEqual StatusReply("OK")
        client(SetRange(key, 6, "Redis"))() mustEqual IntegerReply(11)
        assertBulkReply(client(Get(key)), "Hello Redis")
      }
      "STRLEN" >> {
        val key = "strlen"
        val value = "Hello World"
        client(Set(key, value))() mustEqual StatusReply("OK")
        client(Strlen(key))() mustEqual IntegerReply(11)
        client(Strlen("nosuchkey"))() mustEqual IntegerReply(0)
      }
    }

    "Handle List Commands" >> {
      "LLEN" >> {
        val key = "llen"
        val value = "Wassup."
        client(LPush(key, Seq(value)))() mustEqual IntegerReply(1)
        client(LLen(key))() mustEqual IntegerReply(1)
        client(LPush(key, Seq(value)))() mustEqual IntegerReply(2)
        client(LLen(key))() mustEqual IntegerReply(2)
      }
      "LINDEX" >> {
        val key = "lindex"
        val value1 = "Wassup."
        val value2 = "different."
        client(LPush(key, Seq(value1)))() mustEqual IntegerReply(1)
        assertBulkReply(client(LIndex(key, 0)), value1)
        client(LPush(key, List(value2)))() mustEqual IntegerReply(2)
        assertBulkReply(client(LIndex(key, 0)), value2)
        assertBulkReply(client(LIndex(key, 1)), value1)
      }
      "LINSERT" >> {
        val key = "linsert"
        val value1 = "Wassup."
        val value2 = "different."
        val value3 = "more diffrent."
        client(LPush(key, List(value1)))() mustEqual IntegerReply(1)
        client(LInsert(key, "BEFORE", value1, value2))() mustEqual IntegerReply(2)
        //client(LInsert(key, "AFTER", StringToBytes(value1), StringToBytes(value3)))() mustEqual IntegerReply(3)
        //assertMBulkReply(client(LRange(key, 0, -1)), List(value2, value1, value3))
      }
      "LPOP" >> {
        val key = "lpop"
        val value1 = "Wassup."
        val value2 = "different."
        client(LPush(key, List(value1)))() mustEqual IntegerReply(1)
        client(LPush(key, List(value2)))() mustEqual IntegerReply(2)
        assertBulkReply(client(LPop(key)), value2)
        assertBulkReply(client(LPop(key)), value1)
      }
      "LPUSH" >> {
        val key = "lpush"
        val value = "Wassup."
        client(LPush(key, List(value)))() mustEqual IntegerReply(1)
        client(LLen(key))() mustEqual IntegerReply(1)
        client(LPush(key, List(value)))() mustEqual IntegerReply(2)
        client(LLen(key))() mustEqual IntegerReply(2)
      }
      "LREM" >> {
        val key = "lrem"
        val value = "Wassup."
        client(LPush(key, List(value)))() mustEqual IntegerReply(1)
        client(LPush(key, List(value)))() mustEqual IntegerReply(2)
        client(LRem(key, 1, value))() mustEqual IntegerReply(1)
        assertMBulkReply(client(LRange(key, 0, -1)), List(value))
      }
      "LSET" >> {
        val key = "lset"
        val value1 = "Wassup."
        val value2 = "Different."
        client(LPush(key, List(value1)))() mustEqual IntegerReply(1)
        client(LPush(key, List(value1)))() mustEqual IntegerReply(2)
        client(LSet(key, 1, value2))() mustEqual StatusReply("OK")
        assertMBulkReply(client(LRange(key, 0, -1)), List(value1, value2))
      }
      "LRANGE" >> {
        val key = "lrange"
        val value = "Wassup."
        client(LPush(key, List(value)))() mustEqual IntegerReply(1)
        client(LPush(key, List(value)))() mustEqual IntegerReply(2)
        assertMBulkReply(client(LRange(key, 0, -1)), List(value, value))
      }
      "RPOP" >> {
        val key = "rpop"
        val value1 = "Wassup."
        val value2 = "different."
        client(LPush(key, List(value1)))() mustEqual IntegerReply(1)
        client(LPush(key, List(value2)))() mustEqual IntegerReply(2)
        assertBulkReply(client(RPop(key)), value1)
        assertBulkReply(client(RPop(key)), value2)
      }
      "RPUSH" >> {
        val key = "rpush"
        val value1 = "Wassup."
        val value2 = "different."
        client(RPush(key, List(value1)))() mustEqual IntegerReply(1)
        client(RPush(key, List(value2)))() mustEqual IntegerReply(2)
        assertBulkReply(client(RPop(key)), value2)
        assertBulkReply(client(RPop(key)), value1)
      }
      "LTRIM" >> {
        val key = "ltrim"
        val value1 = "Wassup."
        val value2 = "different."
        val value3 = "Watssup."
        val value4 = "diffferent."
        client(LPush(key, List(value1)))() mustEqual IntegerReply(1)
        client(LPush(key, List(value2)))() mustEqual IntegerReply(2)
        client(LPush(key, List(value3)))() mustEqual IntegerReply(3)
        client(LTrim(key, 0, 1))() mustEqual StatusReply("OK")
        client(LPush(key, List(value4)))() mustEqual IntegerReply(3)
        client(LTrim(key, 0, 1))() mustEqual StatusReply("OK")
        assertMBulkReply(client(LRange(key, 0, -1)), List(value4, value3))
      }
    }

    "Handle Set Commands" >> {
      "SADD" >> {
        val key = "sadd"
        val value1 = "value"
        val value2 = "newvalue"
        client(SAdd(key, List(value1)))() mustEqual IntegerReply(1)
        client(SAdd(key, List(value1)))() mustEqual IntegerReply(0)
        client(SAdd(key, List(value2)))() mustEqual IntegerReply(1)
      }
      "SMEMBERS" >> {
        val key = "smembers"
        val value1 = "value"
        val value2 = "newvalue"
        client(SAdd(key, List(value1)))() mustEqual IntegerReply(1)
        client(SAdd(key, List(value2)))() mustEqual IntegerReply(1)
        client(SMembers(key))() match {
          case MBulkReply(message) =>
            ReplyFormat.toString(message).toSet mustEqual immutable.Set(value1, value2)
          case EmptyMBulkReply() => true mustEqual false
          case _ => fail("Received incorrect reply type")
        }
        client(SAdd(key, List(value2)))() mustEqual IntegerReply(0)
      }
      "SISMEMBER" >> {
        val key = "sismember"
        val value1 = "value"
        val value2 = "newvalue"
        client(SAdd(key, List(value1)))() mustEqual IntegerReply(1)
        client(SIsMember(key, value1))() mustEqual IntegerReply(1)
        client(SIsMember(key, value2))() mustEqual IntegerReply(0)
        client(SAdd(key, List(value2)))() mustEqual IntegerReply(1)
        client(SIsMember(key, value2))() mustEqual IntegerReply(1)
      }
      "SCARD" >> {
        val key = "scard"
        val value1 = "value"
        val value2 = "newvalue"
        client(SCard(key))() mustEqual IntegerReply(0)
        client(SAdd(key, List(value1)))() mustEqual IntegerReply(1)
        client(SCard(key))() mustEqual IntegerReply(1)
        client(SAdd(key, List(value2)))() mustEqual IntegerReply(1)
        client(SCard(key))() mustEqual IntegerReply(2)
      }
      "SREM" >> {
        val key = "srem"
        val value1 = "value"
        val value2 = "newvalue"
        client(SAdd(key, List(value1)))() mustEqual IntegerReply(1)
        client(SAdd(key, List(value2)))() mustEqual IntegerReply(1)
        client(SIsMember(key, value2))() mustEqual IntegerReply(1)
        client(SIsMember(key, value1))() mustEqual IntegerReply(1)
        client(SRem(key, List(value2)))() mustEqual IntegerReply(1)
        client(SIsMember(key, value2))() mustEqual IntegerReply(0)
      }
      "SPOP" >> {
        val key = "spop"
        val value1 = "value"
        val value2 = "newvalue"
        client(SAdd(key, List(value1)))() mustEqual IntegerReply(1)
        client(SAdd(key, List(value2)))() mustEqual IntegerReply(1)
        client(SCard(key))() mustEqual IntegerReply(2)
        client(SPop(key))()
        client(SCard(key))() mustEqual IntegerReply(1)
        client(SPop(key))()
        client(SCard(key))() mustEqual IntegerReply(0)
      }
    }
  }

  def assertMBulkReply(reply: Future[Reply], expects: List[String], contains: Boolean = false) =
    reply() match {
      case MBulkReply(msgs) => contains match {
        case true =>
          expects.isEmpty must beFalse
          val newMsgs = ReplyFormat.toString(msgs)
          expects.foreach { msg => newMsgs must contain(msg) }
        case false =>
          // expects.isEmpty must beFalse
          ReplyFormat.toChannelBuffers(msgs) map {
            msg => CBToString(msg)
          } mustEqual expects
      }
      case EmptyMBulkReply() => expects.isEmpty must beTrue
      case r: Reply => fail("Expected MBulkReply, got %s".format(r))
      case _ => fail("Expected MBulkReply")
    }

  def assertBulkReply(reply: Future[Reply], expects: String) = reply() match {
    case BulkReply(msg) => BytesToString(msg.array) mustEqual expects
    case _ => fail("Expected BulkReply")
  }

  def assertIntegerReply(reply: Future[Reply], expects: Int, delta: Int = 10) = reply() match {
    case IntegerReply(amnt) => amnt.toInt must beCloseTo(expects, delta)
    case _ => fail("Expected IntegerReply")
  }

  doAfterSpec {
    client.close()
    server.close()
    RedisCluster.stopAll()
  }

}
