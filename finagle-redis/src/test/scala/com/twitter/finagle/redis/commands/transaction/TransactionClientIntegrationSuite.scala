package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.ServerError
import com.twitter.finagle.redis.naggati.RedisClientTest
import com.twitter.finagle.redis.protocol.{ErrorReply, Get, HDel, HMGet, HSet, IntegerReply, Set}
import com.twitter.finagle.redis.tags.{RedisTest, ClientTest}
import com.twitter.finagle.redis.util.ReplyFormat
import com.twitter.util.Await
import org.junit.Ignore
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import java.util.concurrent.locks.ReentrantLock

@Ignore
@RunWith(classOf[JUnitRunner])
final class TransactionClientIntegrationSuite extends RedisClientTest {

  test("Correctly set and get transaction", RedisTest, ClientTest) {
    withRedisClient { client =>
      val txResult = Await.result(
        client.transaction { tx =>
          tx.multi().unit before
          tx.set(foo, bar).unit before
          tx.set(baz, boo).unit before
          tx.exec()
        })
      assert(ReplyFormat.toString(txResult.toList) == Seq("OK", "OK"))
    }
  }

  test("Correctly hash set and multi get transaction", RedisTest, ClientTest) {
    withRedisClient { client =>
      val txResult = Await.result(
        client.transaction { tx =>
          tx.multi().unit before
          tx.hSet(foo, bar, baz).unit before
          tx.hSet(foo, boo, moo).unit before
          tx.hMGet(foo, Seq(bar, boo)).unit before
          tx.exec()
        })
      assert(ReplyFormat.toString(txResult.toList) == Seq("1", "1", "baz", "moo"))
    }
  }

  test("Correctly perform key command on incorrect data type", RedisTest, ClientTest) {
    withRedisClient { client =>
      val txResult = Await.result(
        client.transaction { tx =>
          tx.multi().unit before
          tx.hSet(foo, boo, moo).unit before
          tx.get(foo).unit before
          tx.hDel(foo, Seq(boo)).unit before
          tx.exec()
        })
      txResult.toList match {
        case Seq(IntegerReply(1), ErrorReply(message), IntegerReply(1)) =>
          // TODO: the exact error message varies in different versions of redis. fix this later
          assert(message endsWith "Operation against a key holding the wrong kind of value")
      }
    }
  }

  test("Correctly fail after a watched key is modified", RedisTest, ClientTest) {
    withRedisClient { client =>
      intercept[ServerError] {
        Await.result(
        client.transaction { tx =>
          tx.watch(Seq(foo)).unit before
          tx.set(foo, boo).unit before
          tx.multi().unit before
          tx.get(foo).unit before
          tx.exec()
        })
      }
    }
  }

  test("Correctly watch then unwatch a key", RedisTest, ClientTest) {
    withRedisClient { client =>
      val txResult = Await.result(
        client.transaction { tx =>
          tx.set(foo, bar).unit before
          tx.watch(Seq(foo)).unit before
          tx.set(foo, boo).unit before
          tx.unwatch().unit before
          tx.multi().unit before
          tx.get(foo).unit before
          tx.exec()
        })
      assert(ReplyFormat.toString(txResult.toList) == Seq("boo"))
    }
  }

  test("Correctly perform a set followed by get on the same key", RedisTest, ClientTest) {
    withRedisClient { client =>
      val txResult = Await.result(
        client.transaction { tx =>
          tx.multi().unit before
          tx.set(foo, bar).unit before
          tx.get(foo).unit before
          tx.exec()
        })
      assert(ReplyFormat.toString(txResult.toList) == Seq("OK", "bar"))
    }
  }
}
