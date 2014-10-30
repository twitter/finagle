package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.naggati.RedisClientServerIntegrationTest
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.tags.{ClientServerTest, RedisTest}
import com.twitter.util.Await
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class ListClientServerIntegrationSuite extends RedisClientServerIntegrationTest {

  test("LLEN should return the length of the list", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val actualFirstLPushReply = Await.result(client(LPush(foo, List(bar))))
      val expectedFirstLPushReply = IntegerReply(1)
      assert(actualFirstLPushReply === expectedFirstLPushReply)

      val actualFirstLlenReply = Await.result(client(LLen(foo)))
      val expectedFirstLlenReply = IntegerReply(1)
      assert(actualFirstLlenReply === expectedFirstLlenReply)

      val actualSecondLPushReply = Await.result(client(LPush(foo, List(bar))))
      val expectedSecondLPushReply = IntegerReply(2)
      assert(actualSecondLPushReply === expectedSecondLPushReply)

      val actualSecondLlenReply = Await.result(client(LLen(foo)))
      val expectedSecondLlenReply = IntegerReply(2)
      assert(actualSecondLlenReply === expectedSecondLlenReply)
    }
  }

  test("LINDEX should get an element from a list by its index", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val actualFirstLPushReply = Await.result(client(LPush(foo, List(bar))))
      val expectedFirstLPushReply = IntegerReply(1)
      assert(actualFirstLPushReply === expectedFirstLPushReply)

      val actualFirstLIndexReply = client(LIndex(foo, 0))
      val expectedFirstLIndexReply = chanBuf2String(bar)
      assertBulkReply(actualFirstLIndexReply, expectedFirstLIndexReply)

      val actualSecondLPushReply = Await.result(client(LPush(foo, List(baz))))
      val expectedSecondLPushReply = IntegerReply(2)
      assert(actualSecondLPushReply === expectedSecondLPushReply)

      val actualSecondLIndexReply = client(LIndex(foo, 0))
      val expectedSecondLIndexReply = chanBuf2String(baz)
      assertBulkReply(actualSecondLIndexReply, expectedSecondLIndexReply)

      val actualThirdLIndexReply = client(LIndex(foo, 1))
      val expectedThirdLIndexReply = chanBuf2String(bar)
      assertBulkReply(actualThirdLIndexReply, expectedThirdLIndexReply)
    }
  }

  test("LINSERT should insert an element before or after another element in a list",
    ClientServerTest, RedisTest) {
      withRedisClient { client =>
        val actualFirstLPushReply = Await.result(client(LPush(foo, List(bar))))
        val expectedFirstLPushReply = IntegerReply(1)
        assert(actualFirstLPushReply === expectedFirstLPushReply)

        val actualLInsertReply = Await.result(client(LInsert(foo, "BEFORE", bar, moo)))
        val expectedLInsertReply = IntegerReply(2)
        assert(actualLInsertReply == expectedLInsertReply)
      }
  }

  test("LPOP should remove and get the first element in a list", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val actualFirstLPushReply = Await.result(client(LPush(foo, List(bar))))
      val expectedFirstLPushReply = IntegerReply(1)
      assert(actualFirstLPushReply === expectedFirstLPushReply)

      val actualSecondLPushReply = Await.result(client(LPush(foo, List(moo))))
      val expectedSecondLPushReply = IntegerReply(2)
      assert(actualSecondLPushReply === expectedSecondLPushReply)

      val actualFirstLPopReply = client(LPop(foo))
      val expectedFirstLPopReply = chanBuf2String(moo)
      assertBulkReply(actualFirstLPopReply, expectedFirstLPopReply)

      val actualSecondLPopReply = client(LPop(foo))
      val expectedSecondLPopReply = chanBuf2String(bar)
      assertBulkReply(actualSecondLPopReply, expectedSecondLPopReply)
    }
  }

  test("LPUSH should prepend one or multiple values to a list", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val actualFirstLPushReply = Await.result(client(LPush(foo, List(bar))))
      val expectedFirstLPushReply = IntegerReply(1)
      assert(actualFirstLPushReply === expectedFirstLPushReply)

      val actualFirstLlenReply = Await.result(client(LLen(foo)))
      val expectedFirstLlenReply = IntegerReply(1)
      assert(actualFirstLlenReply === expectedFirstLlenReply)

      val actualSecondLPushReply = Await.result(client(LPush(foo, List(baz))))
      val expectedSecondLPushReply = IntegerReply(2)
      assert(actualSecondLPushReply === expectedSecondLPushReply)

      val actualSecondLlenReply = Await.result(client(LLen(foo)))
      val expectedSecondLlenReply = IntegerReply(2)
      assert(actualSecondLlenReply === expectedSecondLlenReply)
    }
  }

  test("LREM should remove elements from a list", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val actualFirstLPushReply = Await.result(client(LPush(foo, List(bar))))
      val expectedFirstLPushReply = IntegerReply(1)
      assert(actualFirstLPushReply === expectedFirstLPushReply)

      val actualSecondLPushReply = Await.result(client(LPush(foo, List(bar))))
      val expectedSecondLPushReply = IntegerReply(2)
      assert(actualSecondLPushReply === expectedSecondLPushReply)

      val actualLRemReply = Await.result(client(LRem(foo, 1, bar)))
      val expectedLRemReply = IntegerReply(1)
      assert(actualLRemReply === expectedLRemReply)

      val actualLRangeReply = client(LRange(foo, 0, -1))
      val expectedLRangeReply = List(chanBuf2String(bar))
      assertMBulkReply(actualLRangeReply, expectedLRangeReply)
    }
  }

  test("LSET should et the value of an element in a list by its index", ClientServerTest,
    RedisTest) {
      withRedisClient { client =>
        val actualFirstLPushReply = Await.result(client(LPush(foo, List(bar))))
        val expectedFirstLPushReply = IntegerReply(1)
        assert(actualFirstLPushReply === expectedFirstLPushReply)

        val actualSecondLPushReply = Await.result(client(LPush(foo, List(bar))))
        val expectedSecondLPushReply = IntegerReply(2)
        assert(actualSecondLPushReply === expectedSecondLPushReply)

        val actualLSetReply = Await.result(client(LSet(foo, 1, baz)))
        assert(actualLSetReply === OKStatusReply)

        val actualLRangeReply = client(LRange(foo, 0, -1))
        val expectedLRangeReply = List(chanBuf2String(bar), chanBuf2String(baz))
        assertMBulkReply(actualLRangeReply, expectedLRangeReply)
      }
  }

  test("LRANGE should get a range of elements from a list", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val actualFirstLPushReply = Await.result(client(LPush(foo, List(bar))))
      val expectedFirstLPushReply = IntegerReply(1)
      assert(actualFirstLPushReply === expectedFirstLPushReply)

      val actualSecondLPushReply = Await.result(client(LPush(foo, List(bar))))
      val expectedSecondLPushReply = IntegerReply(2)
      assert(actualSecondLPushReply === expectedSecondLPushReply)

      val actualLRangeReply = client(LRange(foo, 0, -1))
      val expectedLRangeReply = List(chanBuf2String(bar), chanBuf2String(bar))
      assertMBulkReply(actualLRangeReply, expectedLRangeReply)
    }
  }

  test("RPOP should remove and get the last element in a list", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val actualFirstLPushReply = Await.result(client(LPush(foo, List(bar))))
      val expectedFirstLPushReply = IntegerReply(1)
      assert(actualFirstLPushReply === expectedFirstLPushReply)

      val actualSecondLPushReply = Await.result(client(LPush(foo, List(baz))))
      val expectedSecondLPushReply = IntegerReply(2)
      assert(actualSecondLPushReply === expectedSecondLPushReply)

      val actualFirstRPopReply = client(RPop(foo))
      val expectedFirstRPopReply = chanBuf2String(bar)
      assertBulkReply(actualFirstRPopReply, expectedFirstRPopReply)

      val actualSecondRPopReply = client(RPop(foo))
      val expectedSecondRPopReply = chanBuf2String(baz)
      assertBulkReply(actualSecondRPopReply, expectedSecondRPopReply)
    }
  }

  test("RPUSH should append one or multiple values to a list", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val actualFirstRPushResult = Await.result(client(RPush(foo, List(moo))))
      val expectedFirstRPushResult = IntegerReply(1)
      assert(actualFirstRPushResult === expectedFirstRPushResult)

      val actualSecondRPushResult = Await.result(client(RPush(foo, List(boo))))
      val expectedSecondRPushResult = IntegerReply(2)
      assert(actualSecondRPushResult === expectedSecondRPushResult)
    }
  }

  test("LTRIM should trim a list to the specified range", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val actualFirstLPushReply = Await.result(client(LPush(foo, List(moo))))
      val expectedFirstLPushReply = IntegerReply(1)
      assert(actualFirstLPushReply === expectedFirstLPushReply)

      val actualSecondLPushReply = Await.result(client(LPush(foo, List(boo))))
      val expectedSecondLPushReply = IntegerReply(2)
      assert(actualSecondLPushReply === expectedSecondLPushReply)

      val actualThirdLPushReply = Await.result(client(LPush(foo, List(baz))))
      val expectedThirdLPushReply = IntegerReply(3)
      assert(actualThirdLPushReply === expectedThirdLPushReply)

      val actualFirstLTrimReply = Await.result(client(LTrim(foo, 0, 1)))
      assert(actualFirstLTrimReply === OKStatusReply)

      val actualFourthLPushReply = Await.result(client(LPush(foo, List(bar))))
      val expectedFourthLPushReply = IntegerReply(3)
      assert(actualFourthLPushReply === expectedFourthLPushReply)

      val actualSecondLTrimReply = Await.result(client(LTrim(foo, 0, 1)))
      assert(actualSecondLTrimReply === OKStatusReply)

      val actualLRangeReply = client(LRange(foo, 0, -1))
      val expectedLRangeReply = List(chanBuf2String(bar), chanBuf2String(baz))
      assertMBulkReply(actualLRangeReply, expectedLRangeReply)
    }
  }
}
