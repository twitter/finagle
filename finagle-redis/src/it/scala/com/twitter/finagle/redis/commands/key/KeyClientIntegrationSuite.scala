package com.twitter.finagle.redis.integration

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.redis.RedisClientTest
import com.twitter.finagle.redis.tags.{RedisTest, ClientTest}
import com.twitter.io.Buf
import com.twitter.util.{Await, Future}
import java.util.Arrays

final class KeyClientIntegrationSuite extends RedisClientTest {

  def await[A](a: Future[A]): A = Await.result(a, 5.seconds)

  test("Correctly perform the DEL command", RedisTest, ClientTest) {
    withRedisClient { client =>
      await(client.set(bufFoo, bufBar))
      await(client.dels(Seq(bufFoo)))
      assert(await(client.get(bufFoo)) == None)
    }
  }

  ignore("Correctly perform the DUMP command", RedisTest, ClientTest) {
    withRedisClient { client =>
      val k = Buf.Utf8("mykey")
      val v = Buf.Utf8("10")
      val key = Buf.Utf8("mykey")
      val value = Buf.Utf8("10")
      val expectedBytes: Array[Byte] =
        Array(0, -64, 10, 7, 0, -111, -83, -126, -74, 6, 100, -74, -95)

      await(client.set(k, v))
      val actualResult =
        await(client.dump(key))
          .fold(fail("Expected result for DUMP"))(Buf.ByteArray.Owned.extract(_))
      assert(Arrays.equals(actualResult, expectedBytes))
      await(client.dels(Seq(bufFoo)))
      assert(await(client.dump(bufFoo)) == None)
    }
  }

  // Once the scan/hscan pull request gets merged into Redis master,
  // the tests can be uncommented.
  test("Correctly perform the SCAN command", RedisTest, ClientTest) {
    withRedisClient { client =>
      await(client.set(bufFoo, bufBar))
      await(client.set(bufBaz, bufBoo))

      val res = await(client.scans(0L, None, None))
      val resList = res.flatMap(Buf.Utf8.unapply).sorted
      assert(resList == Seq("0", "baz", "foo"))

      val withCount = await(client.scans(0, Some(10), None))
      val withCountList = withCount.flatMap(Buf.Utf8.unapply).sorted
      assert(withCountList == Seq("0", "baz", "foo"))

      val pattern = Buf.Utf8("b*")
      val withPattern = await(client.scans(0, None, Some(pattern)))
      val withPatternList = withPattern.flatMap(Buf.Utf8.unapply)
      assert(withPatternList == Seq("0", "baz"))
    }
  }

  test("Correctly perform the EXISTS command", RedisTest, ClientTest) {
    withRedisClient { client =>
      await(client.set(bufFoo, bufBar))
      assert(await(client.exists(bufFoo)) == true)
    }
  }

  test("Correctly perform the TTL command", RedisTest, ClientTest) {
    withRedisClient { client =>
      await(client.set(bufFoo, bufBar))
      val time = 20L

      assert(await(client.expire(bufFoo, time)) == true)

      val result = await(client.ttl(bufFoo)) match {
        case Some(num) => num
        case None => fail("Could not retrieve key for TTL test")
      }
      assert(result <= time)
    }
  }

  test("Correctly perform the EXPIREAT command", RedisTest, ClientTest) {
    withRedisClient { client =>
      await(client.set(bufFoo, bufBar))

      // TODO: this isn't actually a TTL, which means that the second assertion
      // below is true for uninteresting reasons.
      val ttl = System.currentTimeMillis() + 20000L

      assert(await(client.expireAt(bufFoo, ttl)) == true)

      val result = await(client.ttl(bufFoo)) match {
        case Some(num) => num
        case None => fail("Could not retrieve key for TTL")
      }
      assert(result <= ttl)
    }
  }

  test("Correctly perform the MOVE command", RedisTest, ClientTest) {
    withRedisClient { client =>
      val fromDb = 14
      val toDb = 15
      await(client.select(toDb))
      await(client.dels(Seq(bufFoo)))
      await(client.select(fromDb))

      // This following fails with an exceptions since bar is not a database.
      // assert(Await.result(client.move(bufFoo, bufBar)) == false)

      await(client.set(bufFoo, bufBar))
      assert(await(client.move(bufFoo, Buf.Utf8(toDb.toString))) == true)

      await(client.dels(Seq(bufFoo))) // clean up
    }
  }

  test("Correctly perform the PEXPIRE & PTL commands", RedisTest, ClientTest) {
    withRedisClient { client =>
      val ttl = 100000L
      await(client.set(bufFoo, bufBar))
      assert(await(client.pExpire(bufFoo, ttl)) == true)

      val result = await(client.pTtl(bufFoo)) match {
        case Some(num) => num
        case None => fail("Could not retrieve pTtl for key")
      }
      assert(result <= ttl)
    }
  }

  test("Correctly perform the PEXPIREAT & PTL commands", RedisTest, ClientTest) {
    withRedisClient { client =>
      val horizon = 20000L
      val ttl = System.currentTimeMillis() + horizon
      await(client.set(bufFoo, bufBar))
      assert(await(client.pExpireAt(bufFoo, ttl)) == true)

      val result = await(client.pTtl(bufFoo)) match {
        case Some(num) => num
        case None => fail("Could not retrieve pTtl for key")
      }
      assert(result <= horizon)
    }
  }

  test("Correctly perform the PERSIST command", RedisTest, ClientTest) {
    withRedisClient { client =>
      val horizon = 20000L
      val ttl = System.currentTimeMillis() + horizon
      await(client.set(bufFoo, bufBar))
      assert(await(client.pExpireAt(bufFoo, ttl)) == true)

      val result = await(client.pTtl(bufFoo)) match {
        case Some(num) => num
        case None => fail("Could not retrieve pTtl for key")
      }
      assert(result <= horizon)

      val persistResult = await(client.persist(bufFoo))
      assert(persistResult == 1)
      val pttlResult = await(client.pTtl(bufFoo))
      assert(pttlResult != None && pttlResult.get == -1) // indicates key exists, but no ttl set.

      val persistResultNoKey = await(client.persist(bufBar))
      assert(persistResultNoKey == 0)
    }
  }
}
