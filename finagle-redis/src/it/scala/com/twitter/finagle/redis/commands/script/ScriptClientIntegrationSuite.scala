package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.ScriptCommands._
import com.twitter.finagle.redis.RedisClientTest
import com.twitter.finagle.redis.protocol.Reply
import com.twitter.finagle.redis.{Client, ServerError}
import com.twitter.io.Buf
import com.twitter.util.{Await, Future}
import java.math.BigInteger

class ScriptClientIntegrationSuite extends RedisClientTest {
  def stringToBuffer(s: String): Buf = Buf.Utf8(s)

  def stringsToBuffers(s: String*): Seq[Buf] =
    s.map(stringToBuffer)

  def bufferToString(buffer: Buf): String =
    Buf.Utf8.unapply(buffer).get

  def buffersToStrings(buffers: Seq[Buf]): Seq[String] =
    buffers.map(bufferToString)

  def toHexString(bytes: Array[Byte]) =
    String.format("%040x", new BigInteger(1, bytes))

  def SHA1hexString(buffer: Buf): String = {
    val md = java.security.MessageDigest.getInstance("SHA-1")
    val underlying = new Array[Byte](buffer.length)
    buffer.write(underlying, 0)
    toHexString((md.digest(underlying)))
  }

  def SHA1hex(buffer: Buf): Buf =
    stringToBuffer(SHA1hexString(buffer))

  val scriptSet = "redis.call('set', KEYS[1], ARGV[1])"

  val scriptSetReturn = "return redis.call('set', KEYS[1], ARGV[1])"

  val scriptGet = "return redis.call('get', KEYS[1])"

  val scriptGetLong = "return tonumber(redis.call('get', KEYS[1]))"

  val scriptUse =
    """
      |local v = redis.call('decrby', KEYS[1], ARGV[1])
      |if v >= 0 then
      |  return v
      |else
      |  local x = redis.call('incrby', KEYS[1], ARGV[1])
      |  assert(x == v+tonumber(ARGV[1]), 'something wrong!')
      |  error(KEYS[1] .. ' < ' .. ARGV[1] .. ' by: ' .. v)
      |end
    """.stripMargin

  val scriptCompare =
    """
      |local v = tonumber(redis.call('get', KEYS[1]))
      |return v >= tonumber(ARGV[1])
    """.stripMargin

  val scriptTest =
    """
      |local v = tonumber(redis.call('get', KEYS[1]))
      |if v > tonumber(ARGV[1]) then
      |  return { ["ok"] = "1" }
      |elseif v == tonumber(ARGV[1]) then
      |  return { ["err"] = KEYS[1] .. " == " .. ARGV[1] }
      |end
    """.stripMargin

  val scriptHGetAll = "return redis.call('hgetall', KEYS[1])"

  val scriptHMGet = "return redis.call('hmget', KEYS[1], unpack(ARGV))"

  val scriptFactorial =
    """
      |local function fact(n)
      |  if n == 0 then
      |    return 1
      |  else
      |    return n * fact(n-1)
      |  end
      |end
      |redis.call('set', KEYS[1], fact( tonumber(ARGV[1]) ) )
    """.stripMargin

  def testScriptAndSha[T, X](
    testName: String,
    scripts: Seq[Buf],
    cast: Reply => T,
    testCase: (String, Seq[Buf], Client => (Buf, Seq[Buf], Seq[Buf]) => Future[T]) => X
  ) = {
    val sha1s = scripts.map(SHA1hex)

    def noSha1sExist(client: Client) = {
      assert(!Await.result(client.scriptExists(sha1s: _*)).reduce(_ || _))
    }

    def allSha1sExist(client: Client) = {
      assert(Await.result(client.scriptExists(sha1s: _*)).reduce(_ && _))
    }

    test("In " + testName + ", scriptExists should return false in the beginning") {
      withRedisClient {
        noSha1sExist
      }
    }

    testCase(
      testName,
      scripts,
      { client => (s, keys, argv) => client.eval(s, keys, argv) map cast })

    test(
      "In " + testName + ", scriptExists should return true for executed scripts; and return false after scriptFlush"
    ) {
      withRedisClient { client =>
        allSha1sExist(client)

        Await.result(client.scriptFlush())
        noSha1sExist(client)
      }
    }

    test(
      "In " + testName + ", scriptLoad should return SHA1 hex string of scripts; and make scriptExists return true"
    ) {
      withRedisClient { client =>
        val digests = Await.result(Future.collect(scripts map { s => client.scriptLoad(s) }))
        assert(digests == sha1s)

        allSha1sExist(client)
      }
    }

    testCase(
      testName + " SHA w/o fallback",
      scripts,
      { client => (s, keys, argv) =>
        client.evalSha(SHA1hex(s), keys, argv) map cast
      })

    test(
      testName + " SHA w/o fallback should make scriptsExists return true; and scriptFlush should make scriptExists return false"
    ) {
      withRedisClient { client =>
        allSha1sExist(client)

        Await.result(client.scriptFlush())
        noSha1sExist(client)
      }
    }

    testCase(
      testName + " SHA w/ fallback",
      scripts,
      { client => (s, keys, argv) =>
        client.evalSha(SHA1hex(s), s, keys, argv) map cast
      })

    test(
      testName + " SHA w/ fallback should make scriptsExists return true; and scriptFlush should make scriptExists return false again"
    ) {
      withRedisClient { client =>
        allSha1sExist(client)
        Await.result(client.scriptFlush())
        noSha1sExist(client)
      }
    }
  }

  val k1 = stringToBuffer("k1")
  val v1 = stringToBuffer("v1")

  def testScriptUnit[T](
    testName: String,
    scripts: Seq[Buf],
    eval: Client => (Buf, Seq[Buf], Seq[Buf]) => Future[T]
  ) = {
    val Seq(scriptSet, scriptSetReturn, scriptFactorial) = scripts
    test(testName + " should succeed without return value") {
      withRedisClient { client =>
        assert(Await.result(for {
          _ <- eval(client)(scriptSet, Seq(k1), Seq(v1))
          v <- client.get(k1)
        } yield v) == Some(v1))
      }
    }

    test(testName + " should succeed with return value status(OK)") {
      withRedisClient { client =>
        assert(Await.result(for {
          _ <- eval(client)(scriptSetReturn, Seq(k1), Seq(v1))
          v <- client.get(k1)
        } yield v) == Some(v1))
      }
    }

    test(testName + " should succeed with complex recursive function") {
      withRedisClient { client =>
        assert(Await.result(for {
          _ <- eval(client)(scriptFactorial, Seq(k1), stringsToBuffers("4"))
          v <- client.get(k1)
        } yield v) == Some(stringToBuffer("24")))
      }
    }
  }

  testScriptAndSha(
    "eval to Unit",
    stringsToBuffers(scriptSet, scriptSetReturn, scriptFactorial),
    castReply[Unit], // test conversion with explicit castReply[T] function call
    testScriptUnit
  )

  def testScriptLong[T](
    testName: String,
    scripts: Seq[Buf],
    eval: Client => (Buf, Seq[Buf], Seq[Buf]) => Future[T]
  ) = {
    val Seq(scriptGetLong, scriptUse) = scripts
    val money = stringToBuffer("money")

    test(testName + "should succeed with Long return value") {
      withRedisClient { client =>
        assert(Await.result(for {
          _ <- client.set(k1, stringToBuffer("1"))
          v <- eval(client)(scriptGetLong, Seq(k1), Nil)
        } yield v) == 1)
      }
    }

    test(testName + " should succeed with Long return value for complex script") {
      withRedisClient { client =>
        assert(Await.result(for {
          _ <- client.set(money, stringToBuffer("10"))
          v <- eval(client)(scriptUse, Seq(money), stringsToBuffers("7"))
        } yield v) == 3)
      }
    }

    test(testName + "should fail on error thrown by script") {
      withRedisClient { client =>
        Await.result(client.set(money, stringToBuffer("6")))
        assert(Await.result(eval(client)(scriptUse, Seq(money), stringsToBuffers("100")) rescue {
          case ServerError(message) =>
            assert(message.contains("money < 100 by: -94"))
            client.get(money)
        }) == Some(stringToBuffer("6")))
      }
    }
  }

  testScriptAndSha(
    "eval to Long",
    stringsToBuffers(scriptGetLong, scriptUse),
    _.cast[Long], // test conversion through the help of CastableReply (implicitly)
    testScriptLong
  )

  def testScriptBool[T](
    testName: String,
    scripts: Seq[Buf],
    eval: Client => (Buf, Seq[Buf], Seq[Buf]) => Future[T]
  ) = {
    val Seq(scriptCompare, scriptTest) = scripts

    test(testName + " should succeed with Boolean return value") {
      withRedisClient { client =>
        assert(
          Await
            .result(for {
              _ <- client.set(k1, stringToBuffer("1"))
              x <- eval(client)(scriptCompare, Seq(k1), stringsToBuffers("1"))
              y <- eval(client)(scriptCompare, Seq(k1), stringsToBuffers("2"))
            } yield (x, y))
            .==((true, false))
        )
      }
    }

    test(testName + " should succeed with status return value and EmptyBulk return value") {
      withRedisClient { client =>
        assert(
          Await
            .result(for {
              _ <- client.set(stringToBuffer("strength"), stringToBuffer("10"))
              x <- eval(client)(scriptTest, stringsToBuffers("strength"), stringsToBuffers("9"))
              y <- eval(client)(scriptTest, stringsToBuffers("strength"), stringsToBuffers("200"))
            } yield (x, y))
            .==((true, false))
        )
      }
    }

    test(testName + " fail on error returned by script") {
      withRedisClient { client =>
        assert(Await.result(for {
          _ <- client.set(stringToBuffer("gem"), stringToBuffer("1000"))
          x <- eval(client)(scriptTest, stringsToBuffers("gem"), stringsToBuffers("1000")) rescue {
            case ServerError(message) =>
              assert(message == "gem == 1000")
              Future.None
          }
        } yield x) == None)
      }
    }
  }

  testScriptAndSha(
    "eval to Bool",
    stringsToBuffers(scriptCompare, scriptTest),
    _.cast[Boolean],
    testScriptBool
  )

  def testScriptBuffer[T](
    testName: String,
    scripts: Seq[Buf],
    eval: Client => (Buf, Seq[Buf], Seq[Buf]) => Future[T]
  ) = {
    val Seq(scriptGet) = scripts

    test(testName + " succeed with returned Buf") {
      withRedisClient { client =>
        assert(
          Await
            .result(for {
              _ <- client.set(stringToBuffer("username"), stringToBuffer("blah"))
              s1 <- eval(client)(scriptGet, stringsToBuffers("username"), Nil)
              s2 <- eval(client)(scriptGet, stringsToBuffers("nonexisting"), Nil)
            } yield (s1, s2))
            .==((stringToBuffer("blah"), stringToBuffer("")))
        )
      }
    }
  }

  testScriptAndSha(
    "eval to Buffer",
    stringsToBuffers(scriptGet),
    _.cast[Buf],
    testScriptBuffer
  )

  def testScriptBuffers[T](
    testName: String,
    scripts: Seq[Buf],
    eval: Client => (Buf, Seq[Buf], Seq[Buf]) => Future[T]
  ) = {
    val Seq(scriptHGetAll, scriptHMGet) = scripts

    test(testName + " should succeed with returned Seq[Buf]") {
      withRedisClient { client =>
        assert(
          Await
            .result(for {
              _ <- client.hMSet(
                Buf.Utf8("info"),
                Map("x" -> "1", "y" -> "2").map {
                  case (k, v) => Buf.Utf8(k) -> Buf.Utf8(v)
                })
              s1 <- eval(client)(scriptHGetAll, stringsToBuffers("info"), Nil)
              s2 <- eval(client)(scriptHGetAll, stringsToBuffers("nonexsisting"), Nil)
              s3 <- eval(client)(
                scriptHMGet,
                stringsToBuffers("info"),
                stringsToBuffers("a", "b", "c")
              )
              s4 <- eval(client)(scriptHMGet, stringsToBuffers("info"), stringsToBuffers("x", "z"))
              s5 <- client.hMGet(Buf.Utf8("info"), stringsToBuffers("x", "z"))
            } yield (s1, s2, s3, s4, s5))
            .==(
              (
                stringsToBuffers("x", "1", "y", "2"),
                Nil,
                stringsToBuffers("", "", ""),
                stringsToBuffers("1", ""),
                stringsToBuffers("1", "")
              )
            )
        )
      }
    }
  }

  testScriptAndSha(
    "eval to Buffers",
    stringsToBuffers(scriptHGetAll, scriptHMGet),
    _.cast[Seq[Buf]],
    testScriptBuffers
  )
}
