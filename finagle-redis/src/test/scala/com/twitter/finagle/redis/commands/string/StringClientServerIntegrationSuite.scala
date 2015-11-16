package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.naggati.RedisClientServerIntegrationTest
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.tags.{ClientServerTest, RedisTest}
import com.twitter.finagle.redis.util.{BytesToString, StringToChannelBuffer}
import com.twitter.util.Await
import org.jboss.netty.buffer.ChannelBuffer
import org.junit.Ignore
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@Ignore
@RunWith(classOf[JUnitRunner])
final class StringClientServerIntegrationSuite extends RedisClientServerIntegrationTest {
  implicit def convertToChannelBuffer(s: String): ChannelBuffer = StringToChannelBuffer(s)

  test("APPEND should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(Append("append1", "Hello"))) == IntegerReply(5))
      assert(Await.result(client(Append("append1", " World"))) == IntegerReply(11))
      assertBulkReply(client(Get("append1")), "Hello World")
    }
  }

  test("BITCOUNT should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(BitCount("bitcount"))) == IntegerReply(0L))
      assert(Await.result(client(Set("bitcount", "bar"))) == StatusReply("OK"))
      assert(Await.result(client(BitCount("bitcount"))) == IntegerReply(10L))
      assert(Await.result(client(BitCount("bitcount", Some(2), Some(4)))) == IntegerReply(4L))
    }
  }

  test("BITOP should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(SetBit("bitop1", 0, 1))) == IntegerReply(0L))
      assert(Await.result(client(SetBit("bitop1", 3, 1))) == IntegerReply(0L))
      assert(Await.result(client(SetBit("bitop2", 2, 1))) == IntegerReply(0L))
      assert(Await.result(client(SetBit("bitop2", 3, 1))) == IntegerReply(0L))

      assert(Await.result(client(BitOp(BitOp.And, "bitop3", Seq("bitop1", "bitop2")))) ==
        IntegerReply(1L))
      assert(Await.result(client(GetBit("bitop3", 0))) == IntegerReply(0L))
      assert(Await.result(client(GetBit("bitop3", 3))) == IntegerReply(1L))

      assert(Await.result(client(BitOp(BitOp.Or, "bitop3", Seq("bitop1", "bitop2")))) ==
        IntegerReply(1L))
      assert(Await.result(client(GetBit("bitop3", 0))) == IntegerReply(1L))
      assert(Await.result(client(GetBit("bitop3", 1))) == IntegerReply(0L))

      assert(Await.result(client(BitOp(BitOp.Xor, "bitop3", Seq("bitop1", "bitop2")))) ==
        IntegerReply(1L))
      assert(Await.result(client(GetBit("bitop3", 0))) == IntegerReply(1L))
      assert(Await.result(client(GetBit("bitop3", 1))) == IntegerReply(0L))

      assert(Await.result(client(BitOp(BitOp.Not, "bitop3", Seq("bitop1")))) == IntegerReply(1L))
      assert(Await.result(client(GetBit("bitop3", 0))) == IntegerReply(0L))
      assert(Await.result(client(GetBit("bitop3", 1))) == IntegerReply(1L))
      assert(Await.result(client(GetBit("bitop3", 4))) == IntegerReply(1L))
    }
  }

  test("DECR should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(Set(foo, bar))) == StatusReply("OK"))

      assert(Await.result(client(Decr("decr1"))) == IntegerReply(-1))
      assert(Await.result(client(Decr("decr1"))) == IntegerReply(-2))
      assert(Await.result(client(Decr(foo))).isInstanceOf[ErrorReply])
    }
  }

  test("DECRBY should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(Set(foo, bar))) == StatusReply("OK"))

      assert(Await.result(client(DecrBy("decrby1", 1))) == IntegerReply(-1))
      assert(Await.result(client(DecrBy("decrby1", 10))) == IntegerReply(-11))
      assert(Await.result(client(DecrBy(foo, 1))).isInstanceOf[ErrorReply])
    }
  }

  test("GET should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(Set(foo, bar))) == StatusReply("OK"))

      assert(Await.result(client(Get("thing"))).isInstanceOf[EmptyBulkReply])
      assertBulkReply(client(Get(foo)), "bar")

      intercept[ClientError] {
        Await.result(client(Get(null: ChannelBuffer)))
      }

      intercept[ClientError] {
        Await.result(client(Get(null: List[Array[Byte]])))
      }
    }
  }

  test("GETBIT should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(SetBit("getbit", 7, 1))) == IntegerReply(0))
      assert(Await.result(client(GetBit("getbit", 0))) == IntegerReply(0))
      assert(Await.result(client(GetBit("getbit", 7))) == IntegerReply(1))
      assert(Await.result(client(GetBit("getbit", 100))) == IntegerReply(0))
    }
  }

  test("GETRANGE should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val key = StringToChannelBuffer("getrange")
      val value = "This is a string"
      assert(Await.result(client(Set(key, value))) == StatusReply("OK"))
      assertBulkReply(client(GetRange(key, 0, 3)), "This")
      assertBulkReply(client(GetRange(key, -3, -1)), "ing")
      assertBulkReply(client(GetRange(key, 0, -1)), value)
      assertBulkReply(client(GetRange(key, 10, 100)), "string")
    }
  }

  test("GETSET should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val key = StringToChannelBuffer("getset")
      assert(Await.result(client(Incr(key))) == IntegerReply(1))
      assertBulkReply(client(GetSet(key, "0")), "1")
      assertBulkReply(client(Get(key)), "0")
      assert(Await.result(client(GetSet("brandnewkey", "foo"))) == EmptyBulkReply())
    }
  }

  test("INCR should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(Set(foo, bar))) == StatusReply("OK"))

      assert(Await.result(client(Incr("incr1"))) == IntegerReply(1))
      assert(Await.result(client(Incr("incr1"))) == IntegerReply(2))
      assert(Await.result(client(Incr(foo))).isInstanceOf[ErrorReply])
    }
  }

  test("INCRBY should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(Set(foo, bar))) == StatusReply("OK"))

      assert(Await.result(client(IncrBy("incrby1", 1))) == IntegerReply(1))
      assert(Await.result(client(IncrBy("incrby1", 10))) == IntegerReply(11))
      assert(Await.result(client(IncrBy(foo, 1))).isInstanceOf[ErrorReply])
    }
  }

  test("MGET should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(Set(foo, bar))) == StatusReply("OK"))

      val expects = List(
        BytesToString(RedisCodec.NIL_VALUE_BA.array),
        BytesToString(bar.array)
        )
      val req = client(MGet(List(StringToChannelBuffer("thing"), foo)))
      assertMBulkReply(req, expects)
      intercept[ClientError] {
        Await.result(client(MGet(List[ChannelBuffer]())))
      }
    }
  }

  test("MSET should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val input = Map(
        StringToChannelBuffer("thing")   -> StringToChannelBuffer("thang"),
        foo       -> bar,
        StringToChannelBuffer("stuff")   -> StringToChannelBuffer("bleh")
        )
      assert(Await.result(client(MSet(input))) == StatusReply("OK"))
      val req = client(MGet(List(StringToChannelBuffer("thing"), foo,
        StringToChannelBuffer("noexists"),
        StringToChannelBuffer("stuff"))))
      val expects = List("thang", "bar", BytesToString(RedisCodec.NIL_VALUE_BA.array), "bleh")
      assertMBulkReply(req, expects)
    }
  }

  test("MSETNX should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val input1 = Map(
        StringToChannelBuffer("msnx.key1") -> StringToChannelBuffer("Hello"),
        StringToChannelBuffer("msnx.key2") -> StringToChannelBuffer("there")
        )
      assert(Await.result(client(MSetNx(input1))) == IntegerReply(1))
      val input2 = Map(
        StringToChannelBuffer("msnx.key2") -> StringToChannelBuffer("there"),
        StringToChannelBuffer("msnx.key3") -> StringToChannelBuffer("world")
        )
      assert(Await.result(client(MSetNx(input2))) == IntegerReply(0))
      val expects = List("Hello", "there", BytesToString(RedisCodec.NIL_VALUE_BA.array))
      assertMBulkReply(client(MGet(List(StringToChannelBuffer("msnx.key1"),
        StringToChannelBuffer("msnx.key2"),
        StringToChannelBuffer("msnx.key3")))), expects)
    }
  }

  test("PSETEX should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>

      intercept[ClientError] {
        Await.result(client(PSetEx(null, 300000L, "value")))
      }

      intercept[ClientError] {
        Await.result(client(PSetEx("psetex1", 300000L, null)))
      }

      intercept[ClientError] {
        Await.result(client(PSetEx("psetex1", 0L, "value")))
      }
      assert(Await.result(client(PSetEx("psetex1", 300000L, "value"))) == StatusReply("OK"))
    }
  }

  test("SET should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>

      intercept[ClientError] {
        Await.result(client(Set(null, null)))
      }

      intercept[ClientError] {
        Await.result(client(Set("key1", null)))
      }

      intercept[ClientError] {
        Await.result(client(Set(null, "value1")))
      }
    }
  }

  test("SETBIT should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(SetBit("setbit", 7, 1))) == IntegerReply(0))
      assert(Await.result(client(SetBit("setbit", 7, 0))) == IntegerReply(1))
      assertBulkReply(client(Get("setbit")), BytesToString(Array[Byte](0)))
    }
  }

  test("SETEX should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val key = StringToChannelBuffer("setex")
      assert(Await.result(client(SetEx(key, 10, "Hello"))) == StatusReply("OK"))
      Await.result(client(Ttl(key))) match {
          //TODO: match must beCloseTo(10, 2)
        case IntegerReply(seconds) => assert(seconds.toInt - 10 < 2)
        case _ => fail("Expected IntegerReply")
      }
      assertBulkReply(client(Get(key)), "Hello")
    }
  }

  test("SETNX should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val key = "setnx"
      val value1 = "Hello"
      val value2 = "World"
      assert(Await.result(client(SetNx(key, value1))) == IntegerReply(1))
      assert(Await.result(client(SetNx(key, value2))) == IntegerReply(0))
      assertBulkReply(client(Get(key)), value1)
    }
  }

  test("SETRANGE should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val key = "setrange"
      val value = "Hello World"
      assert(Await.result(client(Set(key, value))) == StatusReply("OK"))
      assert(Await.result(client(SetRange(key, 6, "Redis"))) == IntegerReply(11))
      assertBulkReply(client(Get(key)), "Hello Redis")
    }
  }

  test("STRLEN should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val key = "strlen"
      val value = "Hello World"
      assert(Await.result(client(Set(key, value))) == StatusReply("OK"))
      assert(Await.result(client(Strlen(key))) == IntegerReply(11))
      assert(Await.result(client(Strlen("nosuchkey"))) == IntegerReply(0))
    }
  }
}
