package com.twitter.finagle.redis
package protocol
package integration

import com.twitter.finagle.Service
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.util.Future
import java.net.InetSocketAddress
import org.specs.SpecificationWithJUnit
import util._

class ClientServerIntegrationSpec extends SpecificationWithJUnit {
  lazy val svcClient = ClientBuilder()
                .name("redis-client")
                .codec(Redis())
                .hosts(RedisCluster.hostAddresses())
                .hostConnectionLimit(2)
                .retries(2)
                .build()

  implicit def s2b(s: String) = s.getBytes
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


  val KEY = "foo"
  val VALUE = "bar"

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
        client(Del(List("key1", "key2")))() mustEqual IntegerReply(2)
        client(Del(null:List[String]))() must throwA[ClientError]
        client(Del(List(null)))() must throwA[ClientError]
      }
      "EXISTS" >> {
        client(Exists(KEY))() mustEqual IntegerReply(1)
        client(Exists("nosuchkey"))() mustEqual IntegerReply(0)
        client(Exists(null:String))() must throwA[ClientError]
      }
      "EXPIRE" >> {
        client(Expire("key1", 30))() mustEqual IntegerReply(0)
        client(Expire(null, 30))() must throwA[ClientError]
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
        val request = client(Keys("%s*".format(KEY)))
        val expects = List(KEY)
        assertMBulkReply(request, expects, true)
        client(Keys("%s.*".format(KEY)))() mustEqual EmptyMBulkReply()
      }
      "PERSIST" >> {
        client(Persist("nosuchkey"))() mustEqual IntegerReply(0)
        client(Set("persist", "value"))()
        client(Persist("persist"))() mustEqual IntegerReply(0)
        client(Expire("persist", 30))()
        client(Persist("persist"))() mustEqual IntegerReply(1)
        client(Ttl("persist"))() mustEqual IntegerReply(-1)
        client(Del(List("persist")))()
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
        client(Ttl(null:String))() must throwA[ClientError]
        client(Ttl("thing"))() mustEqual IntegerReply(-1)
      }
      "TYPE" >> {
        client(Type(KEY))() mustEqual StatusReply("string")
        client(Type("nosuchkey"))() mustEqual StatusReply("none")
      }
    }

    "Handle Sorted Set Commands" >> {
      val ZKEY = "zkey"
      val ZVAL = List(ZMember(1, "one"), ZMember(2, "two"), ZMember(3, "three"))

      def zAdd(key: String, members: ZMember*) {
        members.foreach { member =>
          client(ZAdd(key, member))() mustEqual IntegerReply(1)
        }
      }

      doBefore {
        ZVAL.foreach { zv =>
          client(ZAdd(ZKEY, zv))() mustEqual IntegerReply(1)
        }
      }
      doAfter {
        client(Del(ZKEY))() mustEqual IntegerReply(1)
      }

      "ZADD" >> {
        client(ZAdd("zadd1", ZMember(1, "one")))() mustEqual IntegerReply(1)
        client(ZAdd("zadd1", ZMember(2, "two")))() mustEqual IntegerReply(1)
        client(ZAdd("zadd1", ZMember(3, "two")))() mustEqual IntegerReply(0)
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
      "DECR" >> {
        client(Decr("decr1"))() mustEqual IntegerReply(-1)
        client(Decr("decr1"))() mustEqual IntegerReply(-2)
        client(Decr(KEY))() must haveClass[ErrorReply]
        client(Decr(null:String))() must throwA[ClientError]
      }
      "DECRBY" >> {
        client(DecrBy("decrby1", 1))() mustEqual IntegerReply(-1)
        client(DecrBy("decrby1", 10))() mustEqual IntegerReply(-11)
        client(DecrBy(KEY, 1))() must haveClass[ErrorReply]
        client(DecrBy(null: String, 1))() must throwA[ClientError]
      }
      "GET" >> {
        client(Get("thing"))() must haveClass[EmptyBulkReply]
        assertBulkReply(client(Get(KEY)), VALUE)
        client(Get(null:String))() must throwA[ClientError]
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
        client(Incr(null:String))() must throwA[ClientError]
      }
      "INCRBY" >> {
        client(IncrBy("incrby1", 1))() mustEqual IntegerReply(1)
        client(IncrBy("incrby1", 10))() mustEqual IntegerReply(11)
        client(IncrBy(KEY, 1))() must haveClass[ErrorReply]
        client(IncrBy(null: String, 1))() must throwA[ClientError]
      }
      "MGET" >> {
        val expects = List(
          BytesToString(RedisCodec.NIL_VALUE_BA),
          VALUE
        )
        val req = client(MGet(List("thing", KEY)))
        assertMBulkReply(req, expects)
        client(MGet(null))() must throwA[ClientError]
        client(MGet(List(null)))() must throwA[ClientError]
      }
      "MSET" >> {
        val input = Map(
          "thing"   -> StringToBytes("thang"),
          KEY       -> StringToBytes(VALUE),
          "stuff"   -> StringToBytes("bleh")
          )
        client(MSet(input))() mustEqual StatusReply("OK")
        val req = client(MGet(List("thing",KEY,"noexists","stuff")))
        val expects = List("thang",VALUE,BytesToString(RedisCodec.NIL_VALUE_BA),"bleh")
        assertMBulkReply(req, expects)
      }
      "MSETNX" >> {
        val input1 = Map(
          "msnx.key1" -> s2b("Hello"),
          "msnx.key2" -> s2b("there")
        )
        client(MSetNx(input1))() mustEqual IntegerReply(1)
        val input2 = Map(
          "msnx.key2" -> s2b("there"),
          "msnx.key3" -> s2b("world")
        )
        client(MSetNx(input2))() mustEqual IntegerReply(0)
        val expects = List("Hello", "there", BytesToString(RedisCodec.NIL_VALUE_BA))
        assertMBulkReply(client(MGet(List("msnx.key1","msnx.key2","msnx.key3"))), expects)
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
          case IntegerReply(seconds) => seconds must beCloseTo(10, 2)
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
  }

  def assertMBulkReply(reply: Future[Reply], expects: List[String], contains: Boolean = false) =
    reply() match {
      case MBulkReply(msgs) => contains match {
        case true =>
          expects.isEmpty must beFalse
          val newMsgs = msgs.map { msg => BytesToString(msg) }
          expects.foreach { msg => newMsgs must contain(msg) }
        case false =>
          expects.isEmpty must beFalse
          msgs.map { msg => BytesToString(msg) } mustEqual expects
      }
      case EmptyMBulkReply() => expects.isEmpty must beTrue
      case r: Reply => fail("Expected MBulkReply, got %s".format(r))
      case _ => fail("Expected MBulkReply")
    }

  def assertBulkReply(reply: Future[Reply], expects: String) = reply() match {
    case BulkReply(msg) => BytesToString(msg) mustEqual expects
    case _ => fail("Expected BulkReply")
  }

  def assertIntegerReply(reply: Future[Reply], expects: Int, delta: Int = 10) = reply() match {
    case IntegerReply(amnt) => amnt must beCloseTo(expects, delta)
    case _ => fail("Expected IntegerReply")
  }

  doAfterSpec {
    client.release()
    server.close()
    RedisCluster.stopAll()
  }

}
