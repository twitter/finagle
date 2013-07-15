package com.twitter.finagle.redis.integration

import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.redis._
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util._
import com.twitter.util.{Await, Future, Time}
import java.net.InetSocketAddress
import org.jboss.netty.buffer.ChannelBuffer
import org.junit.Ignore
import org.specs.SpecificationWithJUnit
import scala.collection.immutable


// TODO(John Sirois): Convert these tests to run conditionally when an env-var is present at the
// least to get CI coverage.
@Ignore("These are ignored in the pom")
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
    Await.result(client(Del(List(KEY))))
    Await.result(client(Set(KEY, VALUE))) mustEqual StatusReply("OK")
  }

  "A client" should {
    "Handle Key Commands" >> {
      "DEL" >> {
        Await.result(client(Set("key1", "val1"))) mustEqual StatusReply("OK")
        Await.result(client(Set("key2", "val2"))) mustEqual StatusReply("OK")
        Await.result(client(Del(List(StringToChannelBuffer("key1"),
          StringToChannelBuffer("key2"))))) mustEqual IntegerReply(2)
        Await.result(client(Del(null:List[ChannelBuffer]))) must throwA[ClientError]
        Await.result(client(Del(List[ChannelBuffer]()))) must throwA[ClientError]
      }
      "EXISTS" >> {
        Await.result(client(Exists(KEY))) mustEqual IntegerReply(1)
        Await.result(client(Exists("nosuchkey"))) mustEqual IntegerReply(0)
        Await.result(client(Exists(null:ChannelBuffer))) must throwA[ClientError]
      }
      "EXPIRE" >> {
        Await.result(client(Expire("key1", 30))) mustEqual IntegerReply(0)
        Await.result(client(Expire(null:ChannelBuffer, 30))) must throwA[ClientError]
        Await.result(client(Expire(KEY, 3600))) mustEqual IntegerReply(1)
        assertIntegerReply(client(Ttl(KEY)), 3600)
      }
      "EXPIREAT" >> {
        import com.twitter.util.Time
        import com.twitter.conversions.time._
        val t = Time.now + 3600.seconds
        Await.result(client(ExpireAt("key1", t))) mustEqual IntegerReply(0)
        Await.result(client(ExpireAt(KEY, t))) mustEqual IntegerReply(1)
        assertIntegerReply(client(Ttl(KEY)), 3600)
        Await.result(client(ExpireAt(null, t))) must throwA[ClientError]
      }
      "KEYS" >> {
         val request = client(Keys("*"))
         val expects = List("foo")
         assertMBulkReply(request, expects, true)
      }
      "PERSIST" >> {
        Await.result(client(Persist("nosuchkey"))) mustEqual IntegerReply(0)
        Await.result(client(Set("persist", "value")))
        Await.result(client(Persist("persist"))) mustEqual IntegerReply(0)
        Await.result(client(Expire("persist", 30)))
        Await.result(client(Persist("persist"))) mustEqual IntegerReply(1)
        Await.result(client(Ttl("persist"))) mustEqual IntegerReply(-1)
        Await.result(client(Del(List(StringToChannelBuffer("persist")))))
      }
      "RENAME" >> {
        Await.result(client(Set("rename1", "foo")))
        assertBulkReply(client(Get("rename1")), "foo")
        Await.result(client(Rename("rename1", "rename1"))) must haveClass[ErrorReply]
        Await.result(client(Rename("nosuchkey", "foo"))) must haveClass[ErrorReply]
        Await.result(client(Rename("rename1", "rename2"))) mustEqual StatusReply("OK")
        assertBulkReply(client(Get("rename2")), "foo")
      }
      "RENAMENX" >> {
        Await.result(client(Set("renamenx1", "foo")))
        assertBulkReply(client(Get("renamenx1")), "foo")
        Await.result(client(RenameNx("renamenx1", "renamenx1"))) must haveClass[ErrorReply]
        Await.result(client(RenameNx("nosuchkey", "foo"))) must haveClass[ErrorReply]
        Await.result(client(RenameNx("renamenx1", "renamenx2"))) mustEqual IntegerReply(1)
        assertBulkReply(client(Get("renamenx2")), "foo")
        Await.result(client(RenameNx("renamenx2", KEY))) mustEqual IntegerReply(0)
      }
      "RANDOMKEY" >> {
        Await.result(client(Randomkey())) must haveClass[BulkReply]
      }
      "TTL" >> { // tested by expire/expireat
        Await.result(client(Ttl(""))) must throwA[ClientError]
        Await.result(client(Ttl("thing"))) mustEqual IntegerReply(-1)
      }
      "TYPE" >> {
        Await.result(client(Type(KEY))) mustEqual StatusReply("string")
        Await.result(client(Type("nosuchkey"))) mustEqual StatusReply("none")
      }
    }

    "Handle Sorted Set Commands" >> {
      val ZKEY = StringToChannelBuffer("zkey")
      val ZVAL = List(ZMember(1, "one"), ZMember(2, "two"), ZMember(3, "three"))

      def zAdd(key: String, members: ZMember*) {
        members.foreach { member =>
          Await.result(client(ZAdd(key, List(member)))) mustEqual IntegerReply(1)
        }
      }

      doBefore {
        ZVAL.foreach { zv =>
          Await.result(client(ZAdd(ZKEY, List(zv)))) mustEqual IntegerReply(1)
        }
      }
      doAfter {
        Await.result(client(Del(List(ZKEY)))) mustEqual IntegerReply(1)
      }

      "ZADD" >> {
        Await.result(client(ZAdd("zadd1", List(ZMember(1, "one"))))) mustEqual IntegerReply(1)
        Await.result(client(ZAdd("zadd1", List(ZMember(2, "two"))))) mustEqual IntegerReply(1)
        Await.result(client(ZAdd("zadd1", List(ZMember(3, "two"))))) mustEqual IntegerReply(0)
        val expected = List("one", "1", "two", "3")
        assertMBulkReply(client(ZRange("zadd1", 0, -1, WithScores)), expected)
        assertMBulkReply(client(ZRange("zadd1", 0, -1)), List("one", "two"))
      }
      "ZCARD" >> {
        Await.result(client(ZCard(ZKEY))) mustEqual IntegerReply(3)
        Await.result(client(ZCard("nosuchkey"))) mustEqual IntegerReply(0)
        Await.result(client(ZCard(KEY))) must haveClass[ErrorReply]
      }
      "ZCOUNT" >> {
        Await.result(client(ZCount(ZKEY, ZInterval.MIN, ZInterval.MAX))) mustEqual IntegerReply(3)
        Await.result(client(ZCount(ZKEY, ZInterval.exclusive(1), ZInterval(3)))) mustEqual IntegerReply(2)
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

        Await.result(client(ZInterStore("out", List(key, key2), Weights(2,3)))) mustEqual IntegerReply(2)
        assertMBulkReply(
          client(ZRange("out", 0, -1, WithScores)),
          List("one", "5", "two", "10"))

        Await.result(client(ZUnionStore("out", List(key, key2), Weights(2,3)))) mustEqual IntegerReply(3)
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
        Await.result(client(ZRank(key, "three"))) mustEqual IntegerReply(2)
        Await.result(client(ZRank(key, "four"))) mustEqual EmptyBulkReply()
        Await.result(client(ZRevRank(key, "one"))) mustEqual IntegerReply(2)
        Await.result(client(ZRevRank(key, "four"))) mustEqual EmptyBulkReply()
      }
      "ZREM" >> {
        val key = "zrem1"
        zAdd(key, ZMember(1, "one"), ZMember(2, "two"), ZMember(3, "three"))
        Await.result(client(ZRem(key, List("two")))) mustEqual IntegerReply(1)
        Await.result(client(ZRem(key, List("nosuchmember")))) mustEqual IntegerReply(0)
        assertMBulkReply(
          client(ZRange(key, 0, -1, WithScores)),
          List("one", "1", "three", "3"))
      }
      "ZREMRANGEBYRANK" >> {
        val key = "zremrangebyrank1"
        zAdd(key, ZMember(1, "one"), ZMember(2, "two"), ZMember(3, "three"))
        Await.result(client(ZRemRangeByRank(key, 0, 1))) mustEqual IntegerReply(2)
        assertMBulkReply(
          client(ZRange(key, 0, -1, WithScores)),
          List("three", "3"))
      }
      "ZREMRANGEBYSCORE" >> {
        val key = "zremrangebyscore1"
        zAdd(key, ZMember(1, "one"), ZMember(2, "two"), ZMember(3, "three"))
        Await.result(client(ZRemRangeByScore(key, ZInterval.MIN, ZInterval.exclusive(2)))) mustEqual IntegerReply(1)
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
        Await.result(client(Append("append1", "Hello"))) mustEqual IntegerReply(5)
        Await.result(client(Append("append1", " World"))) mustEqual IntegerReply(11)
        assertBulkReply(client(Get("append1")), "Hello World")
      }
      "BITCOUNT" >> {
        Await.result(client(BitCount("bitcount"))) mustEqual IntegerReply(0L)
        Await.result(client(Set("bitcount", "bar"))) mustEqual StatusReply("OK")
        Await.result(client(BitCount("bitcount"))) mustEqual IntegerReply(10L)
        Await.result(client(BitCount("bitcount", Some(2), Some(4)))) mustEqual IntegerReply(4L)
      }
      "BITOP" >> {
        Await.result(client(SetBit("bitop1", 0, 1))) mustEqual IntegerReply(0L)
        Await.result(client(SetBit("bitop1", 3, 1))) mustEqual IntegerReply(0L)
        Await.result(client(SetBit("bitop2", 2, 1))) mustEqual IntegerReply(0L)
        Await.result(client(SetBit("bitop2", 3, 1))) mustEqual IntegerReply(0L)

        Await.result(client(BitOp(BitOp.And, "bitop3", Seq("bitop1", "bitop2")))) mustEqual IntegerReply(1L)
        Await.result(client(GetBit("bitop3", 0))) mustEqual IntegerReply(0L)
        Await.result(client(GetBit("bitop3", 3))) mustEqual IntegerReply(1L)

        Await.result(client(BitOp(BitOp.Or, "bitop3", Seq("bitop1", "bitop2")))) mustEqual IntegerReply(1L)
        Await.result(client(GetBit("bitop3", 0))) mustEqual IntegerReply(1L)
        Await.result(client(GetBit("bitop3", 1))) mustEqual IntegerReply(0L)

        Await.result(client(BitOp(BitOp.Xor, "bitop3", Seq("bitop1", "bitop2")))) mustEqual IntegerReply(1L)
        Await.result(client(GetBit("bitop3", 0))) mustEqual IntegerReply(1L)
        Await.result(client(GetBit("bitop3", 1))) mustEqual IntegerReply(0L)

        Await.result(client(BitOp(BitOp.Not, "bitop3", Seq("bitop1")))) mustEqual IntegerReply(1L)
        Await.result(client(GetBit("bitop3", 0))) mustEqual IntegerReply(0L)
        Await.result(client(GetBit("bitop3", 1))) mustEqual IntegerReply(1L)
        Await.result(client(GetBit("bitop3", 4))) mustEqual IntegerReply(1L)
      }
      "DECR" >> {
        Await.result(client(Decr("decr1"))) mustEqual IntegerReply(-1)
        Await.result(client(Decr("decr1"))) mustEqual IntegerReply(-2)
        Await.result(client(Decr(KEY))) must haveClass[ErrorReply]
      }
      "DECRBY" >> {
        Await.result(client(DecrBy("decrby1", 1))) mustEqual IntegerReply(-1)
        Await.result(client(DecrBy("decrby1", 10))) mustEqual IntegerReply(-11)
        Await.result(client(DecrBy(KEY, 1))) must haveClass[ErrorReply]
      }
      "GET" >> {
        Await.result(client(Get("thing"))) must haveClass[EmptyBulkReply]
        assertBulkReply(client(Get(KEY)), "bar")
        Await.result(client(Get(null:ChannelBuffer))) must throwA[ClientError]
        Await.result(client(Get(null:List[Array[Byte]]))) must throwA[ClientError]
      }
      "GETBIT" >> {
        Await.result(client(SetBit("getbit", 7, 1))) mustEqual IntegerReply(0)
        Await.result(client(GetBit("getbit", 0))) mustEqual IntegerReply(0)
        Await.result(client(GetBit("getbit", 7))) mustEqual IntegerReply(1)
        Await.result(client(GetBit("getbit", 100))) mustEqual IntegerReply(0)
      }
      "GETRANGE" >> {
        val key = "getrange"
        val value = "This is a string"
        Await.result(client(Set(key, value))) mustEqual StatusReply("OK")
        assertBulkReply(client(GetRange(key, 0, 3)), "This")
        assertBulkReply(client(GetRange(key, -3, -1)), "ing")
        assertBulkReply(client(GetRange(key, 0, -1)), value)
        assertBulkReply(client(GetRange(key, 10, 100)), "string")
      }
      "GETSET" >> {
        val key = "getset"
        Await.result(client(Incr(key))) mustEqual IntegerReply(1)
        assertBulkReply(client(GetSet(key, "0")), "1")
        assertBulkReply(client(Get(key)), "0")
        Await.result(client(GetSet("brandnewkey", "foo"))) mustEqual EmptyBulkReply()
      }
      "INCR" >> {
        Await.result(client(Incr("incr1"))) mustEqual IntegerReply(1)
        Await.result(client(Incr("incr1"))) mustEqual IntegerReply(2)
        Await.result(client(Incr(KEY))) must haveClass[ErrorReply]
      }
      "INCRBY" >> {
        Await.result(client(IncrBy("incrby1", 1))) mustEqual IntegerReply(1)
        Await.result(client(IncrBy("incrby1", 10))) mustEqual IntegerReply(11)
        Await.result(client(IncrBy(KEY, 1))) must haveClass[ErrorReply]
      }
      "MGET" >> {
        val expects = List(
          BytesToString(RedisCodec.NIL_VALUE_BA.array),
          BytesToString(VALUE.array)
        )
        val req = client(MGet(List(StringToChannelBuffer("thing"), KEY)))
        assertMBulkReply(req, expects)
        Await.result(client(MGet(List[ChannelBuffer]()))) must throwA[ClientError]
      }
      "MSET" >> {
        val input = Map(
          StringToChannelBuffer("thing")   -> StringToChannelBuffer("thang"),
          KEY       -> VALUE,
          StringToChannelBuffer("stuff")   -> StringToChannelBuffer("bleh")
          )
        Await.result(client(MSet(input))) mustEqual StatusReply("OK")
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
        Await.result(client(MSetNx(input1))) mustEqual IntegerReply(1)
        val input2 = Map(
          StringToChannelBuffer("msnx.key2") -> s2b("there"),
          StringToChannelBuffer("msnx.key3") -> s2b("world")
        )
        Await.result(client(MSetNx(input2))) mustEqual IntegerReply(0)
        val expects = List("Hello", "there", BytesToString(RedisCodec.NIL_VALUE_BA.array))
        assertMBulkReply(client(MGet(List(StringToChannelBuffer("msnx.key1"),
          StringToChannelBuffer("msnx.key2"),
          StringToChannelBuffer("msnx.key3")))), expects)
      }
      "PSETEX" >> {
        Await.result(client(PSetEx(null, 300000L, "value"))) must throwA[ClientError]
        Await.result(client(PSetEx("psetex1", 300000L, null))) must throwA[ClientError]
        Await.result(client(PSetEx("psetex1", 0L, "value"))) must throwA[ClientError]
        Await.result(client(PSetEx("psetex1", 300000L, "value"))) mustEqual StatusReply("OK")
      }
      "SET" >> {
        Await.result(client(Set(null, null))) must throwA[ClientError]
        Await.result(client(Set("key1", null))) must throwA[ClientError]
        Await.result(client(Set(null, "value1"))) must throwA[ClientError]
      }
      "SETBIT" >> {
        Await.result(client(SetBit("setbit", 7, 1))) mustEqual IntegerReply(0)
        Await.result(client(SetBit("setbit", 7, 0))) mustEqual IntegerReply(1)
        assertBulkReply(client(Get("setbit")), BytesToString(Array[Byte](0)))
      }
      "SETEX" >> {
        val key = "setex"
        Await.result(client(SetEx(key, 10, "Hello"))) mustEqual StatusReply("OK")
        Await.result(client(Ttl(key))) match {
          case IntegerReply(seconds) => seconds.toInt must beCloseTo(10, 2)
          case _ => fail("Expected IntegerReply")
        }
        assertBulkReply(client(Get(key)), "Hello")
      }
      "SETNX" >> {
        val key = "setnx"
        val value1 = "Hello"
        val value2 = "World"
        Await.result(client(SetNx(key, value1))) mustEqual IntegerReply(1)
        Await.result(client(SetNx(key, value2))) mustEqual IntegerReply(0)
        assertBulkReply(client(Get(key)), value1)
      }
      "SETRANGE" >> {
        val key = "setrange"
        val value = "Hello World"
        Await.result(client(Set(key, value))) mustEqual StatusReply("OK")
        Await.result(client(SetRange(key, 6, "Redis"))) mustEqual IntegerReply(11)
        assertBulkReply(client(Get(key)), "Hello Redis")
      }
      "STRLEN" >> {
        val key = "strlen"
        val value = "Hello World"
        Await.result(client(Set(key, value))) mustEqual StatusReply("OK")
        Await.result(client(Strlen(key))) mustEqual IntegerReply(11)
        Await.result(client(Strlen("nosuchkey"))) mustEqual IntegerReply(0)
      }
    }

    "Handle List Commands" >> {
      "LLEN" >> {
        val key = "llen"
        val value = "Wassup."
        Await.result(client(LPush(key, Seq(value)))) mustEqual IntegerReply(1)
        Await.result(client(LLen(key))) mustEqual IntegerReply(1)
        Await.result(client(LPush(key, Seq(value)))) mustEqual IntegerReply(2)
        Await.result(client(LLen(key))) mustEqual IntegerReply(2)
      }
      "LINDEX" >> {
        val key = "lindex"
        val value1 = "Wassup."
        val value2 = "different."
        Await.result(client(LPush(key, Seq(value1)))) mustEqual IntegerReply(1)
        assertBulkReply(client(LIndex(key, 0)), value1)
        Await.result(client(LPush(key, List(value2)))) mustEqual IntegerReply(2)
        assertBulkReply(client(LIndex(key, 0)), value2)
        assertBulkReply(client(LIndex(key, 1)), value1)
      }
      "LINSERT" >> {
        val key = "linsert"
        val value1 = "Wassup."
        val value2 = "different."
        val value3 = "more diffrent."
        Await.result(client(LPush(key, List(value1)))) mustEqual IntegerReply(1)
        Await.result(client(LInsert(key, "BEFORE", value1, value2))) mustEqual IntegerReply(2)
        //client(LInsert(key, "AFTER", StringToBytes(value1), StringToBytes(value3)))() mustEqual IntegerReply(3)
        //assertMBulkReply(client(LRange(key, 0, -1)), List(value2, value1, value3))
      }
      "LPOP" >> {
        val key = "lpop"
        val value1 = "Wassup."
        val value2 = "different."
        Await.result(client(LPush(key, List(value1)))) mustEqual IntegerReply(1)
        Await.result(client(LPush(key, List(value2)))) mustEqual IntegerReply(2)
        assertBulkReply(client(LPop(key)), value2)
        assertBulkReply(client(LPop(key)), value1)
      }
      "LPUSH" >> {
        val key = "lpush"
        val value = "Wassup."
        Await.result(client(LPush(key, List(value)))) mustEqual IntegerReply(1)
        Await.result(client(LLen(key))) mustEqual IntegerReply(1)
        Await.result(client(LPush(key, List(value)))) mustEqual IntegerReply(2)
        Await.result(client(LLen(key))) mustEqual IntegerReply(2)
      }
      "LREM" >> {
        val key = "lrem"
        val value = "Wassup."
        Await.result(client(LPush(key, List(value)))) mustEqual IntegerReply(1)
        Await.result(client(LPush(key, List(value)))) mustEqual IntegerReply(2)
        Await.result(client(LRem(key, 1, value))) mustEqual IntegerReply(1)
        assertMBulkReply(client(LRange(key, 0, -1)), List(value))
      }
      "LSET" >> {
        val key = "lset"
        val value1 = "Wassup."
        val value2 = "Different."
        Await.result(client(LPush(key, List(value1)))) mustEqual IntegerReply(1)
        Await.result(client(LPush(key, List(value1)))) mustEqual IntegerReply(2)
        Await.result(client(LSet(key, 1, value2))) mustEqual StatusReply("OK")
        assertMBulkReply(client(LRange(key, 0, -1)), List(value1, value2))
      }
      "LRANGE" >> {
        val key = "lrange"
        val value = "Wassup."
        Await.result(client(LPush(key, List(value)))) mustEqual IntegerReply(1)
        Await.result(client(LPush(key, List(value)))) mustEqual IntegerReply(2)
        assertMBulkReply(client(LRange(key, 0, -1)), List(value, value))
      }
      "RPOP" >> {
        val key = "rpop"
        val value1 = "Wassup."
        val value2 = "different."
        Await.result(client(LPush(key, List(value1)))) mustEqual IntegerReply(1)
        Await.result(client(LPush(key, List(value2)))) mustEqual IntegerReply(2)
        assertBulkReply(client(RPop(key)), value1)
        assertBulkReply(client(RPop(key)), value2)
      }
      "RPUSH" >> {
        val key = "rpush"
        val value1 = "Wassup."
        val value2 = "different."
        Await.result(client(RPush(key, List(value1)))) mustEqual IntegerReply(1)
        Await.result(client(RPush(key, List(value2)))) mustEqual IntegerReply(2)
        assertBulkReply(client(RPop(key)), value2)
        assertBulkReply(client(RPop(key)), value1)
      }
      "LTRIM" >> {
        val key = "ltrim"
        val value1 = "Wassup."
        val value2 = "different."
        val value3 = "Watssup."
        val value4 = "diffferent."
        Await.result(client(LPush(key, List(value1)))) mustEqual IntegerReply(1)
        Await.result(client(LPush(key, List(value2)))) mustEqual IntegerReply(2)
        Await.result(client(LPush(key, List(value3)))) mustEqual IntegerReply(3)
        Await.result(client(LTrim(key, 0, 1))) mustEqual StatusReply("OK")
        Await.result(client(LPush(key, List(value4)))) mustEqual IntegerReply(3)
        Await.result(client(LTrim(key, 0, 1))) mustEqual StatusReply("OK")
        assertMBulkReply(client(LRange(key, 0, -1)), List(value4, value3))
      }
    }

    "Handle Set Commands" >> {
      "SADD" >> {
        val key = "sadd"
        val value1 = "value"
        val value2 = "newvalue"
        Await.result(client(SAdd(key, List(value1)))) mustEqual IntegerReply(1)
        Await.result(client(SAdd(key, List(value1)))) mustEqual IntegerReply(0)
        Await.result(client(SAdd(key, List(value2)))) mustEqual IntegerReply(1)
      }
      "SMEMBERS" >> {
        val key = "smembers"
        val value1 = "value"
        val value2 = "newvalue"
        Await.result(client(SAdd(key, List(value1)))) mustEqual IntegerReply(1)
        Await.result(client(SAdd(key, List(value2)))) mustEqual IntegerReply(1)
        Await.result(client(SMembers(key))) match {
          case MBulkReply(message) =>
            ReplyFormat.toString(message).toSet mustEqual immutable.Set(value1, value2)
          case EmptyMBulkReply() => true mustEqual false
          case _ => fail("Received incorrect reply type")
        }
        Await.result(client(SAdd(key, List(value2)))) mustEqual IntegerReply(0)
      }
      "SISMEMBER" >> {
        val key = "sismember"
        val value1 = "value"
        val value2 = "newvalue"
        Await.result(client(SAdd(key, List(value1)))) mustEqual IntegerReply(1)
        Await.result(client(SIsMember(key, value1))) mustEqual IntegerReply(1)
        Await.result(client(SIsMember(key, value2))) mustEqual IntegerReply(0)
        Await.result(client(SAdd(key, List(value2)))) mustEqual IntegerReply(1)
        Await.result(client(SIsMember(key, value2))) mustEqual IntegerReply(1)
      }
      "SCARD" >> {
        val key = "scard"
        val value1 = "value"
        val value2 = "newvalue"
        Await.result(client(SCard(key))) mustEqual IntegerReply(0)
        Await.result(client(SAdd(key, List(value1)))) mustEqual IntegerReply(1)
        Await.result(client(SCard(key))) mustEqual IntegerReply(1)
        Await.result(client(SAdd(key, List(value2)))) mustEqual IntegerReply(1)
        Await.result(client(SCard(key))) mustEqual IntegerReply(2)
      }
      "SREM" >> {
        val key = "srem"
        val value1 = "value"
        val value2 = "newvalue"
        Await.result(client(SAdd(key, List(value1)))) mustEqual IntegerReply(1)
        Await.result(client(SAdd(key, List(value2)))) mustEqual IntegerReply(1)
        Await.result(client(SIsMember(key, value2))) mustEqual IntegerReply(1)
        Await.result(client(SIsMember(key, value1))) mustEqual IntegerReply(1)
        Await.result(client(SRem(key, List(value2)))) mustEqual IntegerReply(1)
        Await.result(client(SIsMember(key, value2))) mustEqual IntegerReply(0)
      }
      "SPOP" >> {
        val key = "spop"
        val value1 = "value"
        val value2 = "newvalue"
        Await.result(client(SAdd(key, List(value1)))) mustEqual IntegerReply(1)
        Await.result(client(SAdd(key, List(value2)))) mustEqual IntegerReply(1)
        Await.result(client(SCard(key))) mustEqual IntegerReply(2)
        Await.result(client(SPop(key)))
        Await.result(client(SCard(key))) mustEqual IntegerReply(1)
        Await.result(client(SPop(key)))
        Await.result(client(SCard(key))) mustEqual IntegerReply(0)
      }
    }
  }

  def assertMBulkReply(reply: Future[Reply], expects: List[String], contains: Boolean = false) =
    Await.result(reply) match {
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

  def assertBulkReply(reply: Future[Reply], expects: String) = Await.result(reply) match {
    case BulkReply(msg) => BytesToString(msg.array) mustEqual expects
    case _ => fail("Expected BulkReply")
  }

  def assertIntegerReply(reply: Future[Reply], expects: Int, delta: Int = 10) = Await.result(reply) match {
    case IntegerReply(amnt) => amnt.toInt must beCloseTo(expects, delta)
    case _ => fail("Expected IntegerReply")
  }

  doAfterSpec {
    client.close()
    server.close()
    RedisCluster.stopAll()
  }

}
