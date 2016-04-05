package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.naggati.RedisClientTest
import com.twitter.finagle.redis.tags.{RedisTest, ClientTest}
import com.twitter.io.Buf
import com.twitter.util.Await
import com.twitter.finagle.redis.util.{BufToString, StringToBuf}
import java.util.Arrays
import org.junit.Ignore
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@Ignore
@RunWith(classOf[JUnitRunner])
final class KeyClientIntegrationSuite extends RedisClientTest {

  test("Correctly perform the DEL command", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.set(bufFoo, bufBar))
      Await.result(client.dels(Seq(bufFoo)))
      assert(Await.result(client.get(bufFoo)) == None)
    }
  }

  test("Correctly perform the DUMP command", RedisTest, ClientTest) {
    withRedisClient { client =>
      val k = Buf.Utf8("mykey")
      val v = Buf.Utf8("10")
      val key = Buf.Utf8("mykey")
      val value = Buf.Utf8("10")
      val expectedBytes: Array[Byte] = Array(0, -64, 10, 6, 0, -8, 114, 63, -59, -5, -5, 95, 40)

      Await.result(client.set(k, v))
      val actualResult =
        Await.result(client.dump(key)).fold(fail("Expected result for DUMP"))(Buf.ByteArray.Owned.extract(_))
      assert(Arrays.equals(actualResult, expectedBytes))
      Await.result(client.dels(Seq(bufFoo)))
      assert(Await.result(client.dump(bufFoo)) == None)
    }
  }

  // Once the scan/hscan pull request gets merged into Redis master,
  // the tests can be uncommented.
  ignore("Correctly perform the SCAN command", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.set(bufFoo, bufBar))
      Await.result(client.set(bufBaz, bufBoo))
      assert(BufToString(Await.result(client.scans(0, None, None)).apply(1)) == "baz")

      val withCount = Await.result(client.scans(0, Some(10), None))
      assert(BufToString(withCount(0)) == "0")
      assert(BufToString(withCount(1)) == "baz")
      assert(BufToString(withCount(2)) == "foo")

      val pattern = StringToBuf("b*")
      val withPattern = Await.result(client.scans(0, None, Some(pattern)))
      assert(BufToString(withPattern(0)) == "0")
      assert(BufToString(withPattern(1)) == "baz")
    }
  }

  test("Correctly perform the EXISTS command", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.set(bufFoo, bufBar))
      assert(Await.result(client.exists(bufFoo)) == true)
    }
  }

  test("Correctly perform the TTL command", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.set(bufFoo, bufBar))
      val time = 20L

      assert(Await.result(client.expire(bufFoo, time)) == true)

      val result = Await.result(client.ttl(bufFoo)) match {
        case Some(num) => num
        case None      => fail("Could not retrieve key for TTL test")
      }
      assert(result <= time)
    }
  }

  test("Correctly perform the EXPIREAT command", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.set(bufFoo, bufBar))

      // TODO: this isn't actually a TTL, which means that the second assertion
      // below is true for uninteresting reasons.
      val ttl = System.currentTimeMillis() + 20000L

      assert(Await.result(client.expireAt(bufFoo, ttl)) == true)

      val result = Await.result(client.ttl(bufFoo)) match {
        case Some(num) => num
        case None      => fail("Could not retrieve key for TTL")
      }
      assert(result <= ttl)
    }
  }

  test("Correctly perform the MOVE command", RedisTest, ClientTest) {
    withRedisClient { client =>
      val fromDb = 14
      val toDb   = 15
      Await.result(client.select(toDb))
      Await.result(client.dels(Seq(bufFoo)))
      Await.result(client.select(fromDb))

      // This following fails with an exceptions since bar is not a database.
      // assert(Await.result(client.move(bufFoo, bufBar)) == false)

      Await.result(client.set(bufFoo, bufBar))
      assert(Await.result(client.move(bufFoo, StringToBuf(toDb.toString))) == true)

      Await.result(client.dels(Seq(bufFoo))) // clean up
    }
  }

  test("Correctly perform the PEXPIRE & PTL commands", RedisTest, ClientTest) {
    withRedisClient { client =>
      val ttl = 100000L
      Await.result(client.set(bufFoo, bufBar))
      assert(Await.result(client.pExpire(bufFoo, ttl)) == true)

      val result = Await.result(client.pTtl(bufFoo)) match {
        case Some(num) => num
        case None      => fail("Could not retrieve pTtl for key")
      }
      assert(result <= ttl)
    }
  }

  test("Correctly perform the PEXPIREAT & PTL commands", RedisTest, ClientTest) {
    withRedisClient { client =>
      val horizon = 20000L
      val ttl = System.currentTimeMillis() + horizon
      Await.result(client.set(bufFoo, bufBar))
      assert(Await.result(client.pExpireAt(bufFoo, ttl)) == true)

      val result = Await.result(client.pTtl(bufFoo)) match {
        case Some(num) => num
        case None      => fail("Could not retrieve pTtl for key")
      }
      assert(result <= horizon)
    }
  }
}
