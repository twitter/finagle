package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.RedisClientTest
import com.twitter.finagle.redis.protocol.BitOp
import com.twitter.finagle.redis.tags.{RedisTest, ClientTest}
import com.twitter.io.Buf
import com.twitter.util.Await

final class StringClientIntegrationSuite extends RedisClientTest {

  test("Correctly perform the APPEND command", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.set(bufFoo, bufBar))
      assert(Await.result(client.append(Buf.Utf8(bufFoo), Buf.Utf8(bufBaz))) == 6)
    }
  }

  test("Correctly perform the DECRBY command", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.set(bufFoo, Buf.Utf8("21")))
      assert(Await.result(client.decrBy(bufFoo, 2)) == 19)
    }
  }

  test("Correctly perform the GETRANGE command", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.set(bufFoo, Buf.Utf8("boing")))
      assert(Buf.Utf8.unapply(Await.result(client.getRange(bufFoo, 0, 2)).get).get == "boi")
    }
  }

  test("Correctly perform the GET & SET commands", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.get(bufFoo)) == None)

      Await.result(client.set(bufFoo, bufBar))
      assert(Buf.Utf8.unapply(Await.result(client.get(bufFoo)).get).get == "bar")
    }
  }

  test("Correctly perform bit operations", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.bitCount(bufFoo)) == 0L)
      assert(Await.result(client.getBit(bufFoo, 0)) == 0L)
      assert(Await.result(client.setBit(bufFoo, 0, 1)) == 0L)
      assert(Await.result(client.getBit(bufFoo, 0)) == 1L)
      assert(Await.result(client.setBit(bufFoo, 0, 0)) == 1L)

      assert(Await.result(client.setBit(bufFoo, 2, 1)) == 0L)
      assert(Await.result(client.setBit(bufFoo, 3, 1)) == 0L)

      assert(Await.result(client.setBit(bufFoo, 8, 1)) == 0L)
      assert(Await.result(client.bitCount(bufFoo)) == 3L)
      assert(Await.result(client.bitCount(bufFoo, Some(0), Some(0))) == 2L)
      assert(Await.result(client.setBit(bufFoo, 8, 0)) == 1L)

      assert(Await.result(client.setBit(bufBar, 0, 1)) == 0L)
      assert(Await.result(client.setBit(bufBar, 3, 1)) == 0L)

      assert(Await.result(client.bitOp(BitOp.And, bufBaz, Seq(bufFoo, bufBar))) == 2L)
      assert(Await.result(client.bitCount(bufBaz)) == 1L)
      assert(Await.result(client.getBit(bufBaz, 0)) == 0L)
      assert(Await.result(client.getBit(bufBaz, 3)) == 1L)

      assert(Await.result(client.bitOp(BitOp.Or, bufBaz, Seq(bufFoo, bufBar))) == 2L)
      assert(Await.result(client.bitCount(bufBaz)) == 3L)
      assert(Await.result(client.getBit(bufBaz, 0)) == 1L)
      assert(Await.result(client.getBit(bufBaz, 1)) == 0L)

      assert(Await.result(client.bitOp(BitOp.Xor, bufBaz, Seq(bufFoo, bufBar))) == 2L)
      assert(Await.result(client.bitCount(bufBaz)) == 2L)
      assert(Await.result(client.getBit(bufBaz, 0)) == 1L)
      assert(Await.result(client.getBit(bufBaz, 1)) == 0L)

      assert(Await.result(client.bitOp(BitOp.Not, bufBaz, Seq(bufFoo))) == 2L)
      assert(Await.result(client.bitCount(bufBaz)) == 14)
      assert(Await.result(client.getBit(bufBaz, 0)) == 1)
      assert(Await.result(client.getBit(bufBaz, 2)) == 0)
      assert(Await.result(client.getBit(bufBaz, 4)) == 1)
    }
  }

  test("Correctly perform getSet commands", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.getSet(bufFoo, bufBar)) == None)
      assert(Await.result(client.get(bufFoo)) == Some(bufBar))
      assert(Await.result(client.getSet(bufFoo, bufBaz)) == Some(bufBar))
      assert(Await.result(client.get(bufFoo)) == Some(bufBaz))
    }
  }

  test("Correctly perform increment commands", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.incr(bufFoo)) == 1L)
      assert(Await.result(client.incrBy(bufFoo, 10L)) == 11L)
      assert(Await.result(client.incrBy(bufBar, 10L)) == 10L)
      assert(Await.result(client.incr(bufBar)) == 11L)
    }
  }

  test("Correctly perform mGet, mSet, and mSetNx commands", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.mSet(Map(bufFoo -> bufBar, bufBar -> bufBaz)))
      assert(
        Await
          .result(client.mGet(Seq(bufFoo, bufBar, bufBaz))) == Seq(Some(bufBar), Some(bufBaz), None)
      )
      assert(
        Await
          .result(client.mSetNx(Map(bufFoo -> bufBar, bufBaz -> bufFoo, bufBoo -> bufMoo))) == false
      )
      assert(Await.result(client.mSetNx(Map(bufBaz -> bufFoo, bufBoo -> bufMoo))) == true)
      assert(Await.result(client.mGet(Seq(bufBaz, bufBoo))) == Seq(Some(bufFoo), Some(bufMoo)))
    }
  }

  test("Correctly perform set variations", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.pSetEx(bufFoo, 10000L, bufBar))
      assert(Await.result(client.get(bufFoo)) == Some(bufBar))
      assert(Await.result(client.ttl(bufFoo)) forall (_ <= 10L))

      Await.result(client.setEx(bufBar, 10L, bufFoo))
      assert(Await.result(client.get(bufBar)) == Some(bufFoo))
      assert(Await.result(client.ttl(bufBar)) forall (_ <= 10L))

      assert(Await.result(client.setNx(bufBaz, bufFoo)) == true)
      assert(Await.result(client.setNx(bufBaz, bufBar)) == false)

      assert(Await.result(client.setRange(bufBaz, 1, bufBaz)) == 4L)
      assert(Await.result(client.get(bufBaz)) == Some(Buf.Utf8("fbaz")))
    }
  }

  test("Correctly perform new set syntax variations", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.setExNx(bufFoo, 10L, bufBar)) == true)
      assert(Await.result(client.get(bufFoo)) == Some(bufBar))
      assert(Await.result(client.ttl(bufFoo)) forall (_ <= 10L))
      assert(Await.result(client.setExNx(bufFoo, 10L, bufBaz)) == false)

      assert(Await.result(client.setPxNx(bufBar, 10000L, bufBaz)) == true)
      assert(Await.result(client.get(bufBar)) == Some(bufBaz))
      assert(Await.result(client.ttl(bufBar)) forall (_ <= 10L))
      assert(Await.result(client.setPxNx(bufBar, 100L, bufBar)) == false)

      assert(Await.result(client.setXx(bufBaz, bufFoo)) == false)
      Await.result(client.set(bufBaz, bufFoo))
      assert(Await.result(client.setXx(bufBaz, bufBar)) == true)
      assert(Await.result(client.get(bufBaz)) == Some(bufBar))

      assert(Await.result(client.setExXx(bufBoo, 10L, bufFoo)) == false)
      Await.result(client.set(bufBoo, bufFoo))
      assert(Await.result(client.setExXx(bufBoo, 10L, bufBar)) == true)
      assert(Await.result(client.get(bufBoo)) == Some(bufBar))
      assert(Await.result(client.ttl(bufBoo)) forall (_ <= 10L))

      assert(Await.result(client.setPxXx(bufMoo, 10000L, bufFoo)) == false)
      Await.result(client.set(bufMoo, bufFoo))
      assert(Await.result(client.setPxXx(bufMoo, 10000L, bufBar)) == true)
      assert(Await.result(client.get(bufMoo)) == Some(bufBar))
      assert(Await.result(client.ttl(bufMoo)) forall (_ <= 10L))

      Await.result(client.setPx(bufNum, 10000L, bufFoo))
      assert(Await.result(client.get(bufNum)) == Some(bufFoo))
      assert(Await.result(client.ttl(bufNum)) forall (_ <= 10L))
    }
  }

  test("Correctly perform strlen", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.strlen(bufFoo)) == 0L)
      Await.result(client.set(bufFoo, bufBar))
      assert(Await.result(client.strlen(bufFoo)) == 3L)
    }
  }
}
