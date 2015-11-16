package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.naggati.RedisClientTest
import com.twitter.finagle.redis.protocol.{ErrorReply, Get, HDel, HMGet, HSet, IntegerReply, Set}
import com.twitter.finagle.redis.tags.{RedisTest, ClientTest}
import com.twitter.util.Await
import com.twitter.finagle.redis.util.ReplyFormat
import org.junit.Ignore
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@Ignore
@RunWith(classOf[JUnitRunner])
final class TransactionClientIntegrationSuite extends RedisClientTest {

  test("Correctly set and get transaction", RedisTest, ClientTest) {
    withRedisClient { client =>
      val txResult = Await.result(client.transaction(Seq(Set(foo, bar), Set(baz, boo))))
      assert(ReplyFormat.toString(txResult.toList) == Seq("OK", "OK"))
    }
  }

  test("Correctly hash set and multi get transaction", RedisTest, ClientTest) {
    withRedisClient { client =>
      val txResult = Await.result(client.transaction(Seq(HSet(foo, bar, baz), HSet(foo, boo, moo),
        HMGet(foo, Seq(bar, boo)))))
      assert(ReplyFormat.toString(txResult.toList) == Seq("1", "1", "baz", "moo"))
    }
  }

  test("Correctly perform key command on incorrect data type", RedisTest, ClientTest) {
    withRedisClient { client =>
      val txResult = Await.result(client.transaction(Seq(HSet(foo, boo, moo), Get(foo),
        HDel(foo, Seq(boo)))))
      txResult.toList match {
        case Seq(IntegerReply(1), ErrorReply(message), IntegerReply(1)) =>
          // TODO: the exact error message varies in different versions of redis. fix this later
          assert(message endsWith "Operation against a key holding the wrong kind of value")
      }
    }
  }

  test("Correctly fail after a watched key is modified", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.set(foo, bar))
      Await.result(client.watch(Seq(foo)))
      Await.result(client.set(foo, boo))
      intercept[ClientError] {
        Await.result(client.transaction(Seq(Get(foo))))
      }
    }
  }

  test("Correctly watch then unwatch a key", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.set(foo, bar))
      Await.result(client.watch(Seq(foo)))
      Await.result(client.set(foo, boo))
      Await.result(client.unwatch())
      val txResult = Await.result(client.transaction(Seq(Get(foo))))
      assert(ReplyFormat.toString(txResult.toList) == Seq("boo"))
    }
  }

  test("Correctly perform a set followed by get on the same key", RedisTest, ClientTest) {
    withRedisClient { client =>
      val txResult = Await.result(client.transaction(Seq(Set(foo, bar), Get(foo))))
      assert(ReplyFormat.toString(txResult.toList) == Seq("OK", "bar"))
    }
  }
}
