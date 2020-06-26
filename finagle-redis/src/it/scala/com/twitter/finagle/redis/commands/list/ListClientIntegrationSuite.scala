package com.twitter.finagle.redis.integration

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.redis.RedisClientTest
import com.twitter.finagle.redis.tags.{ClientTest, RedisTest}
import com.twitter.io.Buf
import com.twitter.util.{Await, Future}

final class ListClientIntegrationSuite extends RedisClientTest {

  def await[A](a: Future[A]): A = Await.result(a, 5.seconds)

  val IndexFailureMessage = "Unknown failure calling Index"
  val PopFailureMessage = "Failure popping element from list"

  test("Correctly push elements onto a list", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(await(client.lPush(bufFoo, List(bufBar))) == 1)
      assert(await(client.lPush(bufFoo, List(bufBaz))) == 2)
    }
  }

  test("Correctly pop elements off a list", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(await(client.lPush(bufFoo, List(bufBar))) == 1)
      assert(await(client.lPush(bufFoo, List(bufBaz))) == 2)
      assert(await(client.lPop(bufFoo)).getOrElse(fail(PopFailureMessage)) == bufBaz)
      assert(await(client.lPop(bufFoo)).getOrElse(fail(PopFailureMessage)) == bufBar)
    }
  }

  test("Correctly measure length of an actively changing list", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = bufFoo

      assert(await(client.lLen(key)) == 0)
      assert(await(client.lPush(bufFoo, List(bufBar))) == 1, "Failed to insert list item.")
      assert(await(client.lLen(key)) == 1)
      assert(await(client.lPop(key)).getOrElse(fail(PopFailureMessage)) == bufBar)
      assert(await(client.lLen(key)) == 0)
    }
  }

  test("Correctly index elements of an actively changing list", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = Buf.Utf8("lindex")

      assert(await(client.lIndex(key, 0L)) == None)
      assert(await(client.lPush(key, List(bufBar))) == 1)
      assert(await(client.lIndex(key, 0L)).getOrElse(fail(IndexFailureMessage)) == bufBar)
      assert(await(client.lPush(key, List(bufBaz))) == 2)
      assert(await(client.lIndex(key, 0L)).getOrElse(fail(IndexFailureMessage)) == bufBaz)
      assert(await(client.lPop(key)).getOrElse(fail(PopFailureMessage)) == bufBaz)
      assert(await(client.lIndex(key, 0L)).getOrElse(fail(IndexFailureMessage)) == bufBar)
      assert(await(client.lPop(key)).getOrElse(fail(PopFailureMessage)) == bufBar)
    }
  }

  test("Correctly insert before & after a pushed element", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = Buf.Utf8("linsert")
      val PivotFailureMessage = "Pivot not found"

      assert(await(client.lPush(key, List(bufMoo))) == 1)
      assert(
        await(client.lInsertAfter(key, bufMoo, bufBar))
          .getOrElse(fail(PivotFailureMessage)) == 2
      )
      assert(
        await(client.lInsertBefore(key, bufMoo, bufFoo))
          .getOrElse(fail(PivotFailureMessage)) == 3
      )
      assert(await(client.lPop(key)).getOrElse(fail(PopFailureMessage)) == bufFoo)
      assert(await(client.lPop(key)).getOrElse(fail(PopFailureMessage)) == bufMoo)
      assert(await(client.lPop(key)).getOrElse(fail(PopFailureMessage)) == bufBar)
    }
  }

  test("Correctly Push -->> Remove -->> Pop elements in order", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = Buf.Utf8("lremove")

      assert(await(client.lPush(key, List(bufBar))) == 1)
      assert(await(client.lPush(key, List(bufBaz))) == 2)
      assert(await(client.lRem(key, 1L, bufBaz)) == 1)
      assert(await(client.lPop(key)).getOrElse(fail(PopFailureMessage)) == bufBar)
    }
  }

  test("Correctly push members, set one, then pop them off in order", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = Buf.Utf8("lset")

      assert(await(client.lPush(key, List(bufBar))) == 1)
      assert(await(client.lPush(key, List(bufBaz))) == 2)

      /*
       * We ingloriously side effect here as the API returns no verification,
       * if the key isn't found in the List a ServerError is thrown
       */
      await(client.lSet(key, 0L, bufMoo))

      assert(await(client.lPop(key)).getOrElse(fail(PopFailureMessage)) == bufMoo)
      assert(await(client.lPop(key)).getOrElse(fail(PopFailureMessage)) == bufBar)
    }
  }

  test("Correctly push members, examine the range, then pop them off", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = Buf.Utf8("lrange")

      assert(await(client.lPush(key, List(bufBar))) == 1)
      assert(await(client.lPush(key, List(bufBaz))) == 2)
      assert(await(client.lRange(key, 0L, -1L)) == List(bufBaz, bufBar))
      assert(await(client.lPop(key)).getOrElse(fail(PopFailureMessage)) == bufBaz)
      assert(await(client.lPop(key)).getOrElse(fail(PopFailureMessage)) == bufBar)
    }
  }

  test("Correctly push members, the poll members queue style (RPOP)", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = Buf.Utf8("rpop")

      assert(await(client.lPush(key, List(bufBar))) == 1)
      assert(await(client.lPush(key, List(bufBaz))) == 2)
      assert(await(client.rPop(key)).getOrElse(fail(PopFailureMessage)) == bufBar)
      assert(await(client.rPop(key)).getOrElse(fail(PopFailureMessage)) == bufBaz)
    }
  }

  test("Correctly push then pop members from the HEAD (RPUSH RPOP)", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = Buf.Utf8("rpush")

      assert(await(client.rPush(key, List(bufBar))) == 1)
      assert(await(client.rPush(key, List(bufBaz))) == 2)
      assert(await(client.rPop(key)).getOrElse(fail(PopFailureMessage)) == bufBaz)
      assert(await(client.rPop(key)).getOrElse(fail(PopFailureMessage)) == bufBar)
    }
  }

  // LRESET test is ignored because most Redis implementations don't support this method
  ignore("Correctly reset list and set TTL (LRESET)", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = Buf.Utf8("lreset")

      await(client.lReset(key, List(bufBar), 100))
      assert(await(client.lRange(key, 0L, -1L)) == List(bufBar))

      await(client.lReset(key, List(bufBaz), 100))
      assert(await(client.lRange(key, 0L, -1L)) == List(bufBaz))

      val ttl = await(client.ttl(key)).get
      assert(90 < ttl && ttl <= 100)
    }
  }

  test("Correctly push and trim members, then pop the two remaining", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = Buf.Utf8("ltrim")

      assert(await(client.lPush(key, List(bufBar))) == 1)
      assert(await(client.lPush(key, List(bufBaz))) == 2)
      assert(await(client.lPush(key, List(bufBoo))) == 3)

      /*
       * We ingloriously side effect here as the API returns no verification,
       * if the key isn't found in the List a ServerError is thrown
       */
      await(client.lTrim(key, 0L, 1L))

      assert(await(client.lPush(key, List(bufMoo))) == 3)

      /*
       * We ingloriously side effect here as the API returns no verification,
       * if the key isn't found in the List a ServerError is thrown
       */
      await(client.lTrim(key, 0L, 1L))

      assert(await(client.rPop(key)).getOrElse(fail(PopFailureMessage)) == bufBoo)
      assert(await(client.rPop(key)).getOrElse(fail(PopFailureMessage)) == bufMoo)
    }
  }
}
