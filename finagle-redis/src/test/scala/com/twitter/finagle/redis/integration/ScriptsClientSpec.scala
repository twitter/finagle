package com.twitter.finagle.redis.integration

import java.math.BigInteger

import com.twitter.io.Charsets
import com.twitter.util.Await.result
import com.twitter.util.Future
import com.twitter.finagle.redis.{Client, ServerError}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.scalatest.{WordSpec, MustMatchers, BeforeAndAfterAll}

// Testing scripting features of redis Client
class ScriptsClientSpec extends WordSpec with MustMatchers with BeforeAndAfterAll with WithRedisClient {
  override def beforeAll(): Unit = {
    startRedisCluster
  }

  override def afterAll(): Unit = {
    stopRedisCluster
  }

  def stringToBuffer(s: String): ChannelBuffer = {
    ChannelBuffers.wrappedBuffer(s.getBytes(Charsets.Utf8))
  }

  def stringsToBuffers(s: String*): Seq[ChannelBuffer] = {
    s map stringToBuffer
  }

  def stringToBufferMap(pairs: (String, String)*): Map[ChannelBuffer, ChannelBuffer] = {
    Map(pairs map {p => (stringToBuffer(p._1), stringToBuffer(p._2))} :_*)
  }

  def bufferToString(buffer: ChannelBuffer): String = {
    buffer.toString(Charsets.Utf8)
  }

  def buffersToStrings(buffers: Seq[ChannelBuffer]): Seq[String] = {
    buffers map bufferToString
  }

//  def bufferToStringOpt(buffers: Option[ChannelBuffer]): Option[String] = {
//    buffers map bufferToString
//  }

  def toHexString(bytes: Array[Byte]) = {
    String.format("%040x", new BigInteger(1, bytes))
  }

  def SHA1hexString(buffer: ChannelBuffer): String = {
    val md = java.security.MessageDigest.getInstance("SHA-1")
    toHexString((md.digest(buffer.array())))
  }

  def SHA1hex(buffer: ChannelBuffer): ChannelBuffer = {
    stringToBuffer(SHA1hexString(buffer))
  }

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

  def testScriptAndSha[T, X](testName: String, scripts: Seq[ChannelBuffer],
                             eval: Client => (ChannelBuffer, Seq[ChannelBuffer], Seq[ChannelBuffer]) => T,
                             evalSha: Client => (ChannelBuffer, Seq[ChannelBuffer], Seq[ChannelBuffer], Option[ChannelBuffer]) => T,
                             test: (String, Seq[ChannelBuffer], Client => (ChannelBuffer, Seq[ChannelBuffer], Seq[ChannelBuffer]) => T) => X) = {
    val sha1s = scripts map SHA1hex

    def noSha1sExist(client: Client) = {
      result(client.scriptExists(sha1s: _*)).reduce(_ || _) mustBe false
    }

    def allSha1sExist(client: Client) = {
      result(client.scriptExists(sha1s: _*)).reduce(_ && _) mustBe true
    }

    "In " + testName + ", scriptExists" should {
      "return false in the beginning" in {
        withRedisClient {
          noSha1sExist
        }
      }
    }

    test(testName, scripts, { client => (s, keys, argv) => eval(client)(s, keys, argv) })

    "In " + testName + ", scriptExists" should {
      "return true for executed scripts" in {
        withRedisClient {
          allSha1sExist
        }
      }

      "return false after scriptFlush" in {
        withRedisClient { client =>
          result(client.scriptFlush())
          noSha1sExist(client)
        }
      }
    }

    "In " + testName + ", scriptLoad" should {
      "return SHA1 hex string of scripts" in {
        withRedisClient { client =>
          val digests = result(Future.collect(scripts map {
            s => client.scriptLoad(s)
          })) mustEqual sha1s
        }
      }

      "make scriptExists return true" in {
        withRedisClient {
          allSha1sExist
        }
      }
    }

    test(testName + " SHA w/o fallback", scripts, { client => (s, keys, argv) => evalSha(client)(SHA1hex(s), keys, argv, None) })

    testName + " SHA w/o fallback" should {
      "make scriptsExists return true" in {
        withRedisClient {
          allSha1sExist
        }
      }
    }

    "In " + testName + ", scriptFlush" should {
      "make scriptExists return false" in {
        withRedisClient { client =>
          result(client.scriptFlush())
          noSha1sExist(client)
        }
      }
    }

    test(testName + " SHA w/ fallback", scripts, { client => (s, keys, argv) => evalSha(client)(SHA1hex(s), keys, argv, Some(s)) })

    testName + " SHA w/ fallback" should {
      "make scriptsExists return true" in {
        withRedisClient {
          allSha1sExist
        }
      }
    }

    "In " + testName + ", scriptFlush" should {
      "make scriptExists return false again" in {
        withRedisClient { client =>
          result(client.scriptFlush())
          noSha1sExist(client)
        }
      }
    }
  }

  val k1 = stringToBuffer("k1")
  val v1 = stringToBuffer("v1")

  def testScriptUnit[T](testName: String,
                        scripts: Seq[ChannelBuffer],
                        eval: Client => (ChannelBuffer, Seq[ChannelBuffer], Seq[ChannelBuffer]) => Future[T]) = {
    val Seq(scriptSet, scriptSetReturn, scriptFactorial) = scripts
    testName should {
      "succeed without return value" in {
        withRedisClient { client =>
          result(for {
            _ <- eval(client)(scriptSet, Seq(k1), Seq(v1))
            v <- client.get(k1)
          } yield v) mustBe Some(v1)
        }
      }

      "succeed with return value status(OK)" in {
        withRedisClient { client =>
          result(for {
            _ <- eval(client)(scriptSetReturn, Seq(k1), Seq(v1))
            v <- client.get(k1)
          } yield v) mustBe Some(v1)
        }
      }

      "succeed with complex recursive function" in {
        withRedisClient { client =>
          result(for {
            _ <- eval(client)(scriptFactorial, Seq(k1), stringsToBuffers("4"))
            v <- client.get(k1)
          } yield v) mustBe Some(stringToBuffer("24"))
        }
      }
    }
  }

  testScriptAndSha(
    "evalUnit",
    stringsToBuffers(scriptSet, scriptSetReturn, scriptFactorial),
    { client => client.evalUnit _ },
    { client => client.evalShaUnit _ },
    testScriptUnit
  )

  def testScriptLong[T](testName: String,
                        scripts: Seq[ChannelBuffer],
                        eval: Client => (ChannelBuffer, Seq[ChannelBuffer], Seq[ChannelBuffer]) => Future[T]) = {
    val Seq(scriptGetLong, scriptUse) = scripts
    val money = stringToBuffer("money")

    testName should {
      "succeed with Long return value" in {
        withRedisClient { client =>
          result(for {
            _ <- client.set(k1, stringToBuffer("1"))
            v <- eval(client)(scriptGetLong, Seq(k1), Nil)
          } yield v) mustBe 1
        }
      }

      "succeed with Long return value for complex script" in {
        withRedisClient { client =>
          result(for {
            _ <- client.set(money, stringToBuffer("10"))
            v <- eval(client)(scriptUse, Seq(money), stringsToBuffers("7"))
          } yield v) mustBe 3
        }
      }

      "fail on error thrown by script" in {
        withRedisClient { client =>
          result(client.set(money, stringToBuffer("6")))
          result(eval(client)(scriptUse, Seq(money), stringsToBuffers("100")) rescue {
            case ServerError(message) =>
              message must include("money < 100 by: -94")
              client.get(money)
          }) mustBe Some(stringToBuffer("6"))
        }
      }
    }
  }

  testScriptAndSha(
    "evalLong",
    stringsToBuffers(scriptGetLong, scriptUse),
    { client => client.evalLong _ },
    { client => client.evalShaLong _ },
    testScriptLong
  )

  def testScriptBool[T](testName: String,
                        scripts: Seq[ChannelBuffer],
                        eval: Client => (ChannelBuffer, Seq[ChannelBuffer], Seq[ChannelBuffer]) => Future[T]) = {
    val Seq(scriptCompare, scriptTest) = scripts

    testName should {
      "succeed with Boolean return value" in {
        withRedisClient { client =>
          result(for {
            _ <- client.set(k1, stringToBuffer("1"))
            x <- eval(client)(scriptCompare, Seq(k1), stringsToBuffers("1"))
            y <- eval(client)(scriptCompare, Seq(k1), stringsToBuffers("2"))
          } yield (x, y)) mustBe(true, false)
        }
      }

      "succeed with status return value and EmptyBulk return value" in {
        withRedisClient { client =>
          result(for {
            _ <- client.set(stringToBuffer("strength"), stringToBuffer("10"))
            x <- eval(client)(scriptTest, stringsToBuffers("strength"), stringsToBuffers("9"))
            y <- eval(client)(scriptTest, stringsToBuffers("strength"), stringsToBuffers("200"))
          } yield (x, y)) mustBe(true, false)
        }
      }

      "fail on error returned by script" in {
        withRedisClient { client =>
          result(for {
            _ <- client.set(stringToBuffer("gem"), stringToBuffer("1000"))
            x <- eval(client)(scriptTest, stringsToBuffers("gem"), stringsToBuffers("1000")) rescue {
              case ServerError(message) =>
                message mustBe "gem == 1000"
                Future.None
            }
          } yield x) mustBe None
        }
      }
    }
  }

  testScriptAndSha(
    "evalBool",
    stringsToBuffers(scriptCompare, scriptTest),
    { client => client.evalBool _ },
    { client => client.evalShaBool _ },
    testScriptBool
  )

  def testScriptBuffer[T](testName: String,
                        scripts: Seq[ChannelBuffer],
                        eval: Client => (ChannelBuffer, Seq[ChannelBuffer], Seq[ChannelBuffer]) => Future[T]) = {
    val Seq(scriptGet) = scripts

    testName should {
      "succeed with returned String" in {
        withRedisClient { client =>
          result(for {
            _ <- client.set(stringToBuffer("username"), stringToBuffer("blah"))
            s1 <- eval(client)(scriptGet, stringsToBuffers("username"), Nil)
            s2 <- eval(client)(scriptGet, stringsToBuffers("nonexisting"), Nil)
          } yield (s1, s2)) mustBe(
            stringToBuffer("blah"),
            stringToBuffer("")
            )
        }
      }
    }
  }

  testScriptAndSha(
    "evalBuffer",
    stringsToBuffers(scriptGet),
    { client => client.evalBuffer _ },
    { client => client.evalShaBuffer _ },
    testScriptBuffer
  )

  def testScriptBuffers[T](testName: String,
                         scripts: Seq[ChannelBuffer],
                         eval: Client => (ChannelBuffer, Seq[ChannelBuffer], Seq[ChannelBuffer]) => Future[T]) = {
    val Seq(scriptHGetAll, scriptHMGet) = scripts

    testName should {
      "succeed with returned Seq[String]" in {
        withRedisClient { client =>
          result(for {
            _ <- client.hMSet(stringToBuffer("info"), stringToBufferMap("x" -> "1", "y" -> "2"))
            s1 <- eval(client)(scriptHGetAll, stringsToBuffers("info"), Nil)
            s2 <- eval(client)(scriptHGetAll, stringsToBuffers("nonexsisting"), Nil)
            s3 <- eval(client)(scriptHMGet, stringsToBuffers("info"), stringsToBuffers("a", "b", "c"))
            s4 <- eval(client)(scriptHMGet, stringsToBuffers("info"), stringsToBuffers("x", "z"))
            s5 <- client.hMGet(stringToBuffer("info"), stringsToBuffers("x", "z"))
          } yield (s1, s2, s3, s4, s5)) mustBe(
            stringsToBuffers("x", "1", "y", "2"),
            Nil,
            stringsToBuffers("", "", ""),
            stringsToBuffers("1", ""),
            stringsToBuffers("1", "")
            )
        }
      }
    }
  }

  testScriptAndSha(
    "evalBuffers",
    stringsToBuffers(scriptHGetAll, scriptHMGet),
    { client => client.evalBuffers _ },
    { client => client.evalShaBuffers _ },
    testScriptBuffers
  )
}
