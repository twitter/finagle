package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.naggati.RedisClientTest
import com.twitter.finagle.redis.protocol.BitOp
import com.twitter.finagle.redis.tags.{RedisTest, ClientTest}
import com.twitter.util.Await
import com.twitter.finagle.redis.util.{CBToString, StringToChannelBuffer}
import org.junit.Ignore
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@Ignore
@RunWith(classOf[JUnitRunner])
final class StringClientIntegrationSuite extends RedisClientTest {

  test("Correctly perform the APPEND command", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.set(foo, bar))
      assert(Await.result(client.append(foo, baz)) == 6)
    }
  }

  test("Correctly perform the DECRBY command", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.set(foo, StringToChannelBuffer("21")))
      assert(Await.result(client.decrBy(foo, 2)) == 19)
    }
  }

  test("Correctly perform the GETRANGE command", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.set(foo, StringToChannelBuffer("boing")))
      assert(CBToString(Await.result(client.getRange(foo, 0, 2)).get) == "boi")
    }
  }

  test("Correctly perform the GET & SET commands", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.get(foo)) == None)

      Await.result(client.set(foo, bar))
      assert(CBToString(Await.result(client.get(foo)).get) == "bar")
    }
  }

  test("Correctly perform bit operations", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.bitCount(foo)) == 0L)
      assert(Await.result(client.getBit(foo, 0)) == 0L)
      assert(Await.result(client.setBit(foo, 0, 1)) == 0L)
      assert(Await.result(client.getBit(foo, 0)) == 1L)
      assert(Await.result(client.setBit(foo, 0, 0)) == 1L)

      assert(Await.result(client.setBit(foo, 2, 1)) == 0L)
      assert(Await.result(client.setBit(foo, 3, 1)) == 0L)

      assert(Await.result(client.setBit(foo, 8, 1)) == 0L)
      assert(Await.result(client.bitCount(foo)) == 3L)
      assert(Await.result(client.bitCount(foo, Some(0), Some(0))) == 2L)
      assert(Await.result(client.setBit(foo, 8, 0)) == 1L)

      assert(Await.result(client.setBit(bar, 0, 1)) == 0L)
      assert(Await.result(client.setBit(bar, 3, 1)) == 0L)

      assert(Await.result(client.bitOp(BitOp.And, baz, Seq(foo, bar))) == 2L)
      assert(Await.result(client.bitCount(baz)) == 1L)
      assert(Await.result(client.getBit(baz, 0)) == 0L)
      assert(Await.result(client.getBit(baz, 3)) == 1L)

      assert(Await.result(client.bitOp(BitOp.Or, baz, Seq(foo, bar))) == 2L)
      assert(Await.result(client.bitCount(baz)) == 3L)
      assert(Await.result(client.getBit(baz, 0)) == 1L)
      assert(Await.result(client.getBit(baz, 1)) == 0L)

      assert(Await.result(client.bitOp(BitOp.Xor, baz, Seq(foo, bar))) == 2L)
      assert(Await.result(client.bitCount(baz)) == 2L)
      assert(Await.result(client.getBit(baz, 0)) == 1L)
      assert(Await.result(client.getBit(baz, 1)) == 0L)

      assert(Await.result(client.bitOp(BitOp.Not, baz, Seq(foo))) == 2L)
      assert(Await.result(client.bitCount(baz)) == 14)
      assert(Await.result(client.getBit(baz, 0)) == 1)
      assert(Await.result(client.getBit(baz, 2)) == 0)
      assert(Await.result(client.getBit(baz, 4)) == 1)
    }
  }

  test("Correctly perform getSet commands", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.getSet(foo, bar)) == None)
      assert(Await.result(client.get(foo)) == Some(bar))
      assert(Await.result(client.getSet(foo, baz)) == Some(bar))
      assert(Await.result(client.get(foo)) == Some(baz))
    }
  }

  test("Correctly perform increment commands", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.incr(foo)) == 1L)
      assert(Await.result(client.incrBy(foo, 10L)) == 11L)
      assert(Await.result(client.incrBy(bar, 10L)) == 10L)
      assert(Await.result(client.incr(bar)) == 11L)
    }
  }

  test("Correctly perform mGet, mSet, and mSetNx commands", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.mSet(Map(foo -> bar, bar -> baz)))
      assert(Await.result(client.mGet(Seq(foo, bar, baz))) == Seq(Some(bar), Some(baz), None))
      assert(Await.result(client.mSetNx(Map(foo -> bar, baz -> foo, boo -> moo))) == false)
      assert(Await.result(client.mSetNx(Map(baz -> foo, boo -> moo))) == true)
      assert(Await.result(client.mGet(Seq(baz, boo))) == Seq(Some(foo), Some(moo)))
    }
  }

  test("Correctly perform set variations", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.pSetEx(foo, 10000L, bar))
      assert(Await.result(client.get(foo)) == Some(bar))
      assert(Await.result(client.ttl(foo)) forall (_ <= 10L))

      Await.result(client.setEx(bar, 10L, foo))
      assert(Await.result(client.get(bar)) == Some(foo))
      assert(Await.result(client.ttl(bar)) forall (_ <= 10L))

      assert(Await.result(client.setNx(baz, foo)) == true)
      assert(Await.result(client.setNx(baz, bar)) == false)

      assert(Await.result(client.setRange(baz, 1, baz)) == 4L)
      assert(Await.result(client.get(baz)) == Some(StringToChannelBuffer("fbaz")))
    }
  }

  test("Correctly perform new set syntax variations", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.setExNx(foo, 10L, bar)) == true)
      assert(Await.result(client.get(foo)) == Some(bar))
      assert(Await.result(client.ttl(foo)) forall (_ <= 10L))
      assert(Await.result(client.setExNx(foo, 10L, baz)) == false)

      assert(Await.result(client.setPxNx(bar, 10000L, baz)) == true)
      assert(Await.result(client.get(bar)) == Some(baz))
      assert(Await.result(client.ttl(bar)) forall (_ <= 10L))
      assert(Await.result(client.setPxNx(bar, 100L, bar)) == false)

      assert(Await.result(client.setXx(baz, foo)) == false)
      Await.result(client.set(baz, foo))
      assert(Await.result(client.setXx(baz, bar)) == true)
      assert(Await.result(client.get(baz)) == Some(bar))

      assert(Await.result(client.setExXx(boo, 10L, foo)) == false)
      Await.result(client.set(boo, foo))
      assert(Await.result(client.setExXx(boo, 10L, bar)) == true)
      assert(Await.result(client.get(boo)) == Some(bar))
      assert(Await.result(client.ttl(boo)) forall (_ <= 10L))

      assert(Await.result(client.setPxXx(moo, 10000L, foo)) == false)
      Await.result(client.set(moo, foo))
      assert(Await.result(client.setPxXx(moo, 10000L, bar)) == true)
      assert(Await.result(client.get(moo)) == Some(bar))
      assert(Await.result(client.ttl(moo)) forall (_ <= 10L))

      Await.result(client.setPx(num, 10000L, foo))
      assert(Await.result(client.get(num)) == Some(foo))
      assert(Await.result(client.ttl(num)) forall (_ <= 10L))
    }
  }

  test("Correctly perform strlen", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.strlen(foo)) == 0L)
      Await.result(client.set(foo, bar))
      assert(Await.result(client.strlen(foo)) == 3L)
    }
  }
}
