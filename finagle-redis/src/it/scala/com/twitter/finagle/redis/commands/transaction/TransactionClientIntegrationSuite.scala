package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.ServerError
import com.twitter.finagle.redis.RedisClientTest
import com.twitter.finagle.redis.protocol.{ErrorReply, Get, HDel, HMGet, HSet, IntegerReply, Set}
import com.twitter.finagle.redis.tags.{RedisTest, ClientTest}
import com.twitter.finagle.redis.util.ReplyFormat
import com.twitter.util.Await

final class TransactionClientIntegrationSuite extends RedisClientTest {

  test("Correctly set and get transaction", RedisTest, ClientTest) {
    withRedisClient { client =>
      val txResult = Await.result(client.transaction(Seq(Set(bufFoo, bufBar), Set(bufBaz, bufBoo))))
      assert(ReplyFormat.toString(txResult.toList) == Seq("OK", "OK"))
    }

    withRedisClient { client =>
      val txResult = Await.result(client.transaction { tx =>
        tx.set(bufFoo, bufBar).unit before
          tx.set(bufBaz, bufBoo)
      })
      assert(ReplyFormat.toString(txResult.toList) == Seq("OK", "OK"))
    }
  }

  test("Correctly hash set and multi get transaction", RedisTest, ClientTest) {
    withRedisClient { client =>
      val txResult = Await.result(
        client.transaction(
          Seq(
            HSet(bufFoo, bufBar, bufBaz),
            HSet(bufFoo, bufBoo, bufMoo),
            HMGet(bufFoo, Seq(bufBar, bufBoo))
          )
        )
      )
      assert(ReplyFormat.toString(txResult.toList) == Seq("1", "1", "baz", "moo"))
    }

    withRedisClient { client =>
      val txResult = Await.result(client.transaction { tx =>
        tx.hSet(bufFoo, bufBar, bufBaz).unit before
          tx.hSet(bufFoo, bufBoo, bufMoo).unit before
          tx.hMGet(bufFoo, Seq(bufBar, bufBoo))
      })
      assert(ReplyFormat.toString(txResult.toList) == Seq("1", "1", "baz", "moo"))
    }
  }

  test("Correctly perform key command on incorrect data type", RedisTest, ClientTest) {
    withRedisClient { client =>
      val txResult = Await.result(
        client
          .transaction(Seq(HSet(bufFoo, bufBoo, bufMoo), Get(bufFoo), HDel(bufFoo, Seq(bufBoo))))
      )
      txResult.toList match {
        case Seq(IntegerReply(1), ErrorReply(message), IntegerReply(1)) =>
          // TODO: the exact error message varies in different versions of redis. fix this later
          assert(message endsWith "Operation against a key holding the wrong kind of value")
      }
    }

    withRedisClient { client =>
      val txResult = Await.result(client.transaction { tx =>
        tx.hSet(bufFoo, bufBoo, bufMoo).unit before
          tx.get(bufFoo).unit before
          tx.hDel(bufFoo, Seq(bufBoo))
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
        Await.result(client.transactionSupport { tx =>
          tx.watches(Seq(bufFoo)).unit before
            tx.set(bufFoo, bufBoo).unit before
            tx.transaction {
              tx.get(bufFoo)
            }
        })
      }
    }
  }

  test("Correctly watch then unwatch a key", RedisTest, ClientTest) {
    withRedisClient { client =>
      val txResult = Await.result(client.transactionSupport { tx =>
        tx.set(bufFoo, bufBar).unit before
          tx.watches(Seq(bufFoo)).unit before
          tx.set(bufFoo, bufBoo).unit before
          tx.unwatch().unit before
          tx.transaction {
            tx.get(bufFoo)
          }
      })
      assert(ReplyFormat.toString(txResult.toList) === Seq("boo"))
    }
  }

  test("Correctly perform a set followed by get on the same key", RedisTest, ClientTest) {
    withRedisClient { client =>
      val txResult = Await.result(client.transaction { tx =>
        tx.set(bufFoo, bufBar).unit before
          tx.get(bufFoo)
      })
      assert(ReplyFormat.toString(txResult.toList) === Seq("OK", "bar"))
    }
  }
}
