package com.twitter.finagle.redis.protocol


import com.twitter.conversions.time._
import com.twitter.finagle.redis.{ClientError, ServerError}
import com.twitter.finagle.redis.naggati.test._
import com.twitter.finagle.redis.util._
import com.twitter.util.Time
import org.specs.SpecificationWithJUnit
import org.jboss.netty.buffer.ChannelBuffer

class NaggatiSpec extends SpecificationWithJUnit {
  import com.twitter.logging.Logger
  Logger.reset()
  val log = Logger()
  log.setUseParentHandlers(false)
  log.setLevel(Logger.ALL)

  val foo = StringToChannelBuffer("foo")
  val bar = StringToChannelBuffer("bar")
  val baz = StringToChannelBuffer("baz")
  val boo = StringToChannelBuffer("boo")
  val moo = StringToChannelBuffer("moo")
  val key = s2cb("key")

  def wrap(s: String) = StringToChannelBuffer(s)
  implicit def s2cb(s: String) = StringToChannelBuffer(s)

  "A Redis Request" should {
    val commandCodec = new CommandCodec
    val (codec, counter) = TestCodec(commandCodec.decode, commandCodec.encode)

    "Properly decode" >> {
      "inline requests" >> {
        "key commands" >> {
          "DEL" >> {
            codec(wrap("DEL foo\r\n")) mustEqual List(Del(List(foo)))
            codec(wrap("DEL foo bar\r\n")) mustEqual List(Del(List(foo, bar)))
            codec(wrap("DEL\r\n")) must throwA[ClientError]
          }
          "EXISTS" >> {
            codec(wrap("EXISTS\r\n")) must throwA[ClientError]
            codec(wrap("EXISTS foo\r\n")) mustEqual List(Exists(foo))
          }
          "EXPIRE" >> {
            codec(wrap("EXPIRE foo 100\r\n")) mustEqual List(Expire("foo", 100))
            codec(wrap("EXPIRE foo -1\r\n")) must throwA[ClientError]
          }
          "EXPIREAT" >> {
            codec(wrap("EXPIREAT foo 100\r\n")) must throwA[ClientError]
            val time = Time.now + 10.seconds
            val foo = s2cb("foo")
            unwrap(codec(wrap("EXPIREAT foo %d\r\n".format(time.inSeconds)))) {
              case ExpireAt(foo, timestamp) =>
              timestamp.inSeconds mustEqual time.inSeconds
            }
          }
          "KEYS" >> {
            codec(wrap("KEYS h?llo\r\n")) mustEqual List(Keys("h?llo"))
          }
          "PERSIST" >> {
            codec(wrap("PERSIST\r\n")) must throwA[ClientError]
            codec(wrap("PERSIST foo\r\n")) mustEqual List(Persist("foo"))
          }
          "PEXPIRE" >> {
            codec(wrap("PEXPIRE foo 100000\r\n")) mustEqual List(PExpire("foo", 100000L))
            codec(wrap("PEXPIRE foo -1\r\n")) must throwA[ClientError]
          }
          "PEXPIREAT" >> {
            codec(wrap("PEXPIREAT foo 100000\r\n")) must throwA[ClientError]
            val time = Time.now + 10.seconds
            val foo = s2cb("foo")
            unwrap(codec(wrap("PEXPIREAT foo %d\r\n".format(time.inMilliseconds)))) {
              case PExpireAt(foo, timestamp) =>
                timestamp.inMilliseconds mustEqual time.inMilliseconds
            }
          }
          "PTTL" >> {
            codec(wrap("PTTL foo\r\n")) mustEqual List(PTtl("foo"))
          }
          "RENAME" >> {
            codec(wrap("RENAME\r\n")) must throwA[ClientError]
            codec(wrap("RENAME foo\r\n")) must throwA[ClientError]
            codec(wrap("RENAME foo bar\r\n")) mustEqual List(Rename("foo", "bar"))
          }
          "RENAMENX" >> {
            codec(wrap("RENAMENX\r\n")) must throwA[ClientError]
            codec(wrap("RENAMENX foo\r\n")) must throwA[ClientError]
            codec(wrap("RENAMENX foo bar\r\n")) mustEqual List(RenameNx("foo", "bar"))
          }
          "RANDOMKEY" >> {
            codec(wrap("RANDOMKEY\r\n")) mustEqual List(Randomkey())
          }
          "TTL" >> {
            codec(wrap("TTL foo\r\n")) mustEqual List(Ttl("foo"))
          }
          "TYPE" >> {
            codec(wrap("TYPE\r\n")) must throwA[ClientError]
            codec(wrap("TYPE foo\r\n")) mustEqual List(Type("foo"))
          }
        } // key commands

        "sorted set commands" >> {
          "ZADD" >> {
            val bad = List(
              "ZADD", "ZADD foo", "ZADD foo 123", "ZADD foo BAD_SCORE bar",
              "ZADD foo 123 bar BAD_SCORE bar")
            bad.foreach { e =>
              codec(wrap("%s\r\n".format(e))) must throwA[ClientError]
            }
            val nums = s2cb("nums")
            unwrap(codec(wrap("ZADD nums 3.14159 pi\r\n"))) {
              case ZAdd(nums, members) =>
                unwrap(members) { case ZMember(3.14159, value) =>
                    BytesToString(value.array) mustEqual "pi"
                }
            }
            unwrap(codec(wrap("ZADD nums 3.14159 pi 2.71828 e\r\n"))) {
              case ZAdd(nums, members) => members.toList match {
                case pi :: e :: Nil =>
                  unwrap(List(pi)) { case ZMember(3.14159, value) =>
                    BytesToString(value.array) mustEqual "pi"
                  }
                  unwrap(List(e)) { case ZMember(2.71828, value) =>
                    BytesToString(value.array) mustEqual "e"
                  }
                case _ => fail("Expected two elements in list")
              }
            }
          } // ZADD

          "ZCARD" >> {
            codec(wrap("ZCARD\r\n")) must throwA[ClientError]
            unwrap(codec(wrap("ZCARD foo\r\n"))) {
              case ZCard(key) => BytesToString(key.array) mustEqual "foo"
            }
          }

          "ZCOUNT" >> {
            val bad = List(
              "ZCOUNT", "ZCOUNT foo", "ZCOUNT foo 1", "ZCOUNT foo 1 bar", "ZCOUNT foo bar 1",
              "ZCOUNT foo -inf foo", "ZCOUNT foo 1 +info", "ZCOUNT foo )1 3", "ZCOUNT foo (1 n")
            bad.foreach { b =>
              codec(wrap("%s\r\n".format(b))) must throwA[ClientError]
            }
            val good = Map(
              "foo -inf +inf" -> ZCount("foo", ZInterval.MIN, ZInterval.MAX),
              "foo (1.0 3.0" -> ZCount("foo", ZInterval.exclusive(1), ZInterval(3))
              )
            good.foreach { case(s,v) =>
              unwrap(codec(wrap("ZCOUNT %s\r\n".format(s)))) {
                case c: Command => c mustEqual v
              }
            }
          }

          "ZINCRBY" >> {
            val bad = List("ZINCRBY", "ZINCRBY key", "ZINCRBY key 1", "ZINCRBY key bad member")
            bad.foreach { b =>
              codec(wrap("%s\r\n".format(b))) must throwA[ClientError]
            }
            val key = s2cb("key")
            unwrap(codec(wrap("ZINCRBY key 2 one\r\n"))) {
              case ZIncrBy(key, 2, member) => BytesToString(member.array) mustEqual "one"
            }
            unwrap(codec(wrap("ZINCRBY key 2.1 one\r\n"))) {
              case ZIncrBy(key, value, member) =>
                value mustEqual 2.1
                BytesToString(member.array) mustEqual "one"
            }
          }

          "ZINTERSTORE/ZUNIONSTORE" >> {
            val bad = List(
              "%s", "%s foo", "%s foo 1 a b",
              "%s foo 2 a b WEIGHTS 2", "%s foo 2 a b WEIGHTS 1",
              "%s foo 2 a b WEIGHTS 2 2 AGGREGATE foo",
              "%s foo 2 a b c WEIGHTS 2 2 2", "%s foo 1 b WEIGHTS 2 AGGREGATE",
              "%s foo 2 a b WEIGHTS 2 2 2", "%s foo 1 a WEIGHTS a",
              "%s foo 1 a WEIGHTS 2 WEIGHTS 3",
              "%s foo 1 a AGGREGATE SUM AGGREGATE MAX")
            List("ZINTERSTORE","ZUNIONSTORE").foreach { cmd =>
              def doCmd(rcmd: String) = codec(wrap(rcmd.format(cmd)))
              def verify(k: String, n: Int)(f: (Seq[ChannelBuffer],Option[Weights],Option[Aggregate]) => Unit): PartialFunction[Command,Unit] =
                cmd match {
                  case "ZINTERSTORE" => {
                    case ZInterStore(k, n, keys, w, a) => f(keys,w,a)
                  }
                  case "ZUNIONSTORE" => {
                    case ZUnionStore(k, n, keys, w, a) => f(keys,w,a)
                  }
                  case _ => throw new Exception("Unhandled type")
                }

              bad.foreach { b =>
                val toTry = b.format(cmd)
                try {
                  codec(wrap("%s\r\n".format(toTry)))
                  fail("Unexpected success for %s".format(toTry))
                } catch {
                  case e: Throwable => e must haveClass[ClientError]
                }
              }
              unwrap(doCmd("%s out 2 zset1 zset2\r\n")) {
                verify("out", 2) { (keys, weights, aggregate) =>
                  keys.map(s => BytesToString(s.array)) mustEqual List("zset1", "zset2")
                  weights must beNone
                  aggregate must beNone
                }
              }
              unwrap(doCmd("%s out 2 zset1 zset2 WEIGHTS 2 3\r\n")) {
                verify("out", 2) { (keys, weights, aggregate) =>
                  keys.map(s => BytesToString(s.array)) mustEqual List("zset1", "zset2")
                  weights must beSome(Weights(2, 3))
                  aggregate must beNone
                }
              }
              unwrap(doCmd("%s out 2 zset1 zset2 aggregate sum\r\n")) {
                verify("out", 2) { (keys, weights, aggregate) =>
                  keys.map(s => BytesToString(s.array)) mustEqual List("zset1", "zset2")
                  aggregate must beSome(Aggregate.Sum)
                  weights must beNone
                }
              }
              unwrap(doCmd("%s out 2 zset1 zset2 weights 2 3 aggregate min\r\n")) {
                verify("out", 2) { (keys, weights, aggregate) =>
                  keys.map(s => BytesToString(s.array)) mustEqual List("zset1", "zset2")
                  weights must beSome(Weights(2, 3))
                  aggregate must beSome(Aggregate.Min)
                }
              }
              unwrap(doCmd("%s out 2 zset1 zset2 aggregate max weights 2 3\r\n")) {
                verify("out", 2) { (keys, weights, aggregate) =>
                  keys.map(s => BytesToString(s.array)) mustEqual List("zset1", "zset2")
                  weights must beSome(Weights(2, 3))
                  aggregate must beSome(Aggregate.Max)
                }
              }
            } // List(Zinterstore...
          } // ZINTERSTORE/ZUNIONSTORE

          "ZRANGE/ZREVRANGE" >> {
            val bad = List(
              "%s", "%s myset", "%s myset 1", "%s myset 1 foo",
              "%s myset foo 1", "%s myset 0 2 blah")
            List("ZRANGE","ZREVRANGE").foreach { cmd =>
              def doCmd(s: String) = codec(wrap("%s\r\n".format(s.format(cmd))))
              def verify(k: String, start: Int, stop: Int, scored: Option[CommandArgument]): PartialFunction[Command,Unit] = {
                cmd match {
                  case "ZRANGE" => {
                    case ZRange(k, start, stop, scored) => true must beTrue
                  }
                  case "ZREVRANGE" => {
                    case ZRevRange(k, start, stop, scored) => true must beTrue
                  }
                }
              }
              bad.foreach { b =>
                val scmd = "%s\r\n".format(b.format(cmd))
                codec(wrap(scmd)) must throwA[ClientError]
              }

              unwrap(doCmd("%s myset 0 -1")) {
                verify("myset", 0, -1, None)
              }
              unwrap(doCmd("%s myset 0 -1 withscores")) {
                verify("myset", 0, -1, Some(WithScores))
              }
              unwrap(doCmd("%s myset 0 -1 WITHSCORES")) {
                verify("myset", 0, -1, Some(WithScores))
              }
            }
            
            // Ensure that, after encoding, the bytes we threw in are the bytes we get back
            def testZRangeBytes(key: Array[Byte]) = {
              val zrange = ZRange.get(org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer(key), 0, 1, None)
              val keyBack = zrange match { 
                case ZRange(key, start, stop, withScores) => key
              }
              val bytesBack = keyBack.toByteBuffer.array
              key.toSeq mustEqual bytesBack.toSeq
            }
            
            val goodKey = Array[Byte](58, 49, 127)
            val nonAsciiKey = Array[Byte](58, 49, -128)
            testZRangeBytes(goodKey)
            testZRangeBytes(nonAsciiKey)
            
          } // ZRANGE

          "ZRANGEBYSCORE/ZREVRANGEBYSCORE" >> {
            val bad = List(
              "%s", "%s key", "%s key -inf", "%s key -inf foo", "%s key foo +inf",
              "%s key 0 1 NOSCORES", "%s key 0 1 LIMOT 1 2", "%s key 0 1 LIMIT foo 1",
              "%s key 0 1 LIMIT 1 foo", "%s key 0 1 WITHSCORES WITHSCORES",
              "%s key 0 1 LIMIT 0 1 LIMIT 0 1 WITHSCORES", "%s key 0 1 LIMIT 0 1 NOSCORES",
              "%s key 0 1 LIMIT 1", "%s key 0 1 LIMIT 0 1 WITHSCORES NOSCORES");
            List("ZRANGEBYSCORE","ZREVRANGEBYSCORE").foreach { cmd =>
              def doCmd(rcmd: String) = codec(wrap("%s\r\n".format(rcmd.format(cmd))))
              def verify(k: String, min: ZInterval, max: ZInterval)(f: (Option[CommandArgument],Option[Limit]) => Unit): PartialFunction[Command,Unit] = {
                cmd match {
                  case "ZRANGEBYSCORE" => {
                    case ZRangeByScore(k, min, max, s, l) => f(s, l)
                  }
                  case "ZREVRANGEBYSCORE" => {
                    case ZRevRangeByScore(k, min, max, s, l) => f(s, l)
                  }
                }
              }

              bad.foreach { b =>
                val cstr = b.format(cmd)
                try {
                  codec(wrap("%s\r\n".format(cstr)))
                  fail("Unexpected success for %s".format(cstr))
                } catch {
                  case e: Throwable => e must haveClass[ClientError]
                }
              } // bad

              unwrap(doCmd("%s myzset -inf +inf")) {
                verify("myzset", ZInterval.MIN, ZInterval.MAX) { (s,l) =>
                  s must beNone
                  l must beNone
                }
              }
              unwrap(doCmd("%s myzset 1 2")) {
                verify("myzset", ZInterval(1), ZInterval(2)) { (s,l) =>
                  s must beNone
                  l must beNone
                }
              }
              unwrap(doCmd("%s myzset (1 2")) {
                verify("myzset", ZInterval.exclusive(1), ZInterval(2)) { (s,l) =>
                  s must beNone
                  l must beNone
                }
              }
              unwrap(doCmd("%s myzset (1 (2")) {
                verify("myzset", ZInterval.exclusive(1), ZInterval.exclusive(2)) { (s,l) =>
                  s must beNone
                  l must beNone
                }
              }
              unwrap(doCmd("%s myzset -inf +inf LIMIT 1 5")) {
                verify("myzset", ZInterval.MIN, ZInterval.MAX) { (s,l) =>
                  s must beNone
                  l must beSome(Limit(1,5))
                }
              }
              unwrap(doCmd("%s myzset -inf +inf LIMIT 3 9 WITHSCORES")) {
                verify("myzset", ZInterval.MIN, ZInterval.MAX) { (s,l) =>
                  s must beSome(WithScores)
                  l must beSome(Limit(3, 9))
                }
              }
            } // List(ZRANGEBYSCORE
          } // ZRANGEBYSCORE

          "ZRANK/ZREVRANK" >> {
            val bad = List("%s", "%s key", "%s key member member")
            List("ZRANK", "ZREVRANK").foreach { cmd =>
              def doCmd(s: String) = codec(wrap("%s\r\n".format(s.format(cmd))))
              def verify(k: String)(f: ChannelBuffer => Unit): PartialFunction[Command,Unit] = {
                cmd match {
                  case "ZRANK" => {
                    case ZRank(k, m) => f(m)
                  }
                  case "ZREVRANK" => {
                    case ZRevRank(k, m) => f(m)
                  }
                }
              }

              bad.foreach { b =>
                val scmd = b.format(cmd)
                val fcmd = "%s\r\n".format(scmd)
                try {
                  codec(wrap(fcmd))
                  fail("Unexpected success for %s".format(scmd))
                } catch {
                  case e: Throwable => e must haveClass[ClientError]
                }
              }
              unwrap(doCmd("%s myzset three")) {
                verify("myzset") { m =>
                  BytesToString(m.array) mustEqual "three"
                }
              }
              unwrap(doCmd("%s myzset four")) {
                verify("myzset") { m =>
                  BytesToString(m.array) mustEqual "four"
                }
              }
            }
          } // ZRANK/ZREVRANK

          "ZREM" >> {
            List("ZREM", "ZREM key").foreach { bad =>
              codec(wrap("%s\r\n".format(bad))) must throwA[ClientError]
            }
            val key = s2cb("key")
            unwrap(codec(wrap("ZREM key member1\r\n"))) {
              case ZRem(key, members) =>
                CBToString.fromList(members) mustEqual List("member1")
            }
            unwrap(codec(wrap("ZREM key member1 member2\r\n"))) {
              case ZRem(key, members) =>
                CBToString.fromList(members) mustEqual List("member1", "member2")
            }
          }

          "ZREMRANGEBYRANK" >> {
            val cmd = "ZREMRANGEBYRANK"
            val bad = List("%s", "%s key", "%s key start", "%s key 1", "%s key 1 stop",
              "%s key start 2")
            bad.foreach { b =>
              codec(wrap("%s\r\n".format(b.format(cmd)))) must throwA[ClientError]
            }
            val key = s2cb("key")
            unwrap(codec(wrap(cmd + " key 0 1\r\n"))) {
              case ZRemRangeByRank(key, start, stop) =>
                start must beEqualTo(0)
                stop must beEqualTo(1)
            }
          }

          "ZREMRANGEBYSCORE" >> {
            val cmd = "ZREMRANGEBYSCORE"
            val bad = List("%s", "%s key", "%s key min", "%s key min max", "%s key ( 1",
              "%s key (1 max")
            bad.foreach { b =>
              codec(wrap("%s\r\n".format(b.format(cmd)))) must throwA[ClientError]
            }
            val key = s2cb("key")
            unwrap(codec(wrap(cmd + " key -inf (2.0\r\n"))) {
              case ZRemRangeByScore(key, min, max) =>
                min mustEqual ZInterval.MIN
                max mustEqual ZInterval.exclusive(2)
            }
          }

          "ZSCORE" >> {
            List("ZSCORE","ZSCORE key").foreach { bad =>
              codec(wrap("%s\r\n".format(bad))) must throwA[ClientError]
            }
            val myset = s2cb("myset")
            unwrap(codec(wrap("ZSCORE myset one\r\n"))) {
              case ZScore(myset, one) => BytesToString(one.array) mustEqual "one"
            }
          }

        } // sorted sets should

        "string commands" >> {
          "APPEND" >> {
            codec(wrap("APPEND\r\n")) must throwA[ClientError]
            codec(wrap("APPEND foo\r\n")) must throwA[ClientError]
            val foo = s2cb("foo")
            unwrap(codec(wrap("APPEND foo bar\r\n"))) {
              case Append(foo, value) => BytesToString(value.array) mustEqual "bar"
            }
          }
          "BITCOUNT" >> {
            codec(wrap("BITCOUNT foo\r\n")) mustEqual List(BitCount("foo"))
            codec(wrap("BITCOUNT foo 0\r\n")) must throwA[ClientError]
            codec(wrap("BITCOUNT foo 0 1\r\n")) mustEqual
              List(BitCount("foo", Some(0), Some(1)))
          }
          "BITOP" >> {
            codec(wrap("BITOP\r\n")) must throwA[ClientError]
            codec(wrap("BITOP AND baz foo bar\r\n")) mustEqual
              List(BitOp(BitOp.And, "baz", Seq(s2cb("foo"), s2cb("bar"))))
            codec(wrap("BITOP NOT foo bar baz\r\n")) must throwA[ClientError]
            codec(wrap("BITOP NOT foo bar\r\n")) mustEqual
              List(BitOp(BitOp.Not, "foo", Seq("bar")))
          }
          "DECR" >> {
            codec(wrap("DECR 1\r\n")) mustEqual List(Decr("1"))
            codec(wrap("DECR foo\r\n")) mustEqual List(Decr("foo"))
            codec(wrap("DECR foo 1\r\n")) must throwA[ClientError]
          }
          "DECRBY" >> {
            codec(wrap("DECRBY foo 1\r\n")) mustEqual List(DecrBy("foo", 1))
            codec(wrap("DECRBY foo 4096\r\n")) mustEqual List(DecrBy("foo", 4096))
            codec(wrap("DECRBY foo\r\n")) must throwA[ClientError]
          }
          "GET" >> {
            codec(wrap("GET foo\r\n")) mustEqual List(Get("foo"))
          }
          "GETBIT" >> {
            codec(wrap("GETBIT\r\n")) must throwA[ClientError]
            codec(wrap("GETBIT foo\r\n")) must throwA[ClientError]
            codec(wrap("GETBIT foo 0\r\n")) mustEqual List(GetBit("foo", 0))
          }
          "GETRANGE" >> {
            codec(wrap("GETRANGE\r\n")) must throwA[ClientError]
            codec(wrap("GETRANGE key\r\n")) must throwA[ClientError]
            codec(wrap("GETRANGE key 0\r\n")) must throwA[ClientError]
            codec(wrap("GETRANGE key 0 5\r\n")) mustEqual List(GetRange("key", 0, 5))
          }
          "GETSET" >> {
            codec(wrap("GETSET\r\n")) must throwA[ClientError]
            codec(wrap("GETSET key\r\n")) must throwA[ClientError]
            val key = s2cb("key")
            unwrap(codec(wrap("GETSET key value\r\n"))) {
              case GetSet(key, value) => BytesToString(value.array) mustEqual "value"
            }
          }
          "INCR" >> {
            codec(wrap("INCR 1\r\n")) mustEqual List(Incr("1"))
            codec(wrap("INCR foo\r\n")) mustEqual List(Incr("foo"))
            codec(wrap("INCR foo 1\r\n")) must throwA[ClientError]
          }
          "INCRBY" >> {
            codec(wrap("INCRBY foo 1\r\n")) mustEqual List(IncrBy("foo", 1))
            codec(wrap("INCRBY foo 4096\r\n")) mustEqual List(IncrBy("foo", 4096))
            codec(wrap("INCRBY foo\r\n")) must throwA[ClientError]
          }
          "MGET" >> {
            codec(wrap("MGET foo bar\r\n")) mustEqual List(MGet(List(foo, bar)))
          }
          "MSETNX" >> {
            codec(wrap("MSETNX\r\n")) must throwA[ClientError]
            codec(wrap("MSETNX foo\r\n")) must throwA[ClientError]
            unwrap(codec(wrap("MSETNX foo bar\r\n"))) {
              case MSetNx(map) =>
                map must haveKey(foo)
                BytesToString(map(foo).array) mustEqual "bar"
            }
          }
          "PSETEX" >> {
            codec(wrap("PSETEX\r\n")) must throwA[ClientError]
            codec(wrap("PSETEX foo 1000 bar\r\n")) mustEqual List(PSetEx("foo", 1000L, "bar"))
          }
          "SET" >> {
            unwrap(codec(wrap("SET foo bar\r\n"))) {
              case Set(foo, bar, _, _, _) => BytesToString(bar.array) mustEqual "bar"
            }
          }
          "SETBIT" >> {
            codec(wrap("SETBIT\r\n")) must throwA[ClientError]
            codec(wrap("SETBIT foo\r\n")) must throwA[ClientError]
            codec(wrap("SETBIT foo 0\r\n")) must throwA[ClientError]
            codec(wrap("SETBIT foo 7 1\r\n")) mustEqual List(SetBit("foo", 7, 1))
          }
          "SETEX" >> {
            codec(wrap("SETEX\r\n")) must throwA[ClientError]
            codec(wrap("SETEX key\r\n")) must throwA[ClientError]
            codec(wrap("SETEX key 30\r\n")) must throwA[ClientError]
            unwrap(codec(wrap("SETEX key 30 value\r\n"))) {
              case SetEx(key, 30, value) => BytesToString(value.array) mustEqual "value"
            }
          }
          "SETNX" >> {
            codec(wrap("SETNX\r\n")) must throwA[ClientError]
            codec(wrap("SETNX key\r\n")) must throwA[ClientError]
            unwrap(codec(wrap("SETNX key value\r\n"))) {
              case SetNx(key, value) => BytesToString(value.array) mustEqual "value"
            }
          }
          "New SET Syntax" >> {
            codec(wrap("SET foo bar 100\r\n")) must throwA[ClientError]
            codec(wrap("SET foo bar EX NX\r\n")) must throwA[ClientError]
            codec(wrap("SET foo bar PX NX\r\n")) must throwA[ClientError]

            unwrap(codec(wrap("SET foo bar EX 10\r\n"))) {
              case Set(key, value, ttl, false, false) =>
                ttl mustEqual Some(InSeconds(10L))
            }

            unwrap(codec(wrap("SET foo bar PX 10000\r\n"))) {
              case Set(key, value, ttl, false, false) =>
                ttl mustEqual Some(InMilliseconds(10000L))
            }

            unwrap(codec(wrap("SET foo bar NX EX 10\r\n"))) {
              case Set(key, value, ttl, true, false) =>
                ttl mustEqual Some(InSeconds(10))
            }

            unwrap(codec(wrap("SET foo bar XX\r\n"))) {
              case Set(key, value, None, false, true) =>
                BytesToString(value.array) mustEqual "bar"
            }
          }
          "SETRANGE" >> {
            codec(wrap("SETRANGE\r\n")) must throwA[ClientError]
            codec(wrap("SETRANGE key\r\n")) must throwA[ClientError]
            codec(wrap("SETRANGE key 0\r\n")) must throwA[ClientError]
            unwrap(codec(wrap("SETRANGE key 0 value\r\n"))) {
              case SetRange(key, 0, value) => BytesToString(value.array) mustEqual "value"
            }
          }
          "STRLEN" >> {
            codec(wrap("STRLEN\r\n")) must throwA[ClientError]
            codec(wrap("STRLEN foo\r\n")) mustEqual List(Strlen("foo"))
          }
        } // string commands
        "list commands" >> {
          "LPUSH" >> {
            unwrap(codec(wrap("LPUSH foo bar\r\n"))) {
              case LPush(key, List(value)) => {
                CBToString(key) mustEqual "foo"
                CBToString(value) mustEqual "bar"
              }
            }
          }
        } // list commands
        "set commands" >> {
          "SADD" >> {
            unwrap(codec(wrap("SADD foo bar\r\n"))) {
              case SAdd(key, List(member)) => {
                CBToString(key) mustEqual "foo"
                CBToString(member) mustEqual "bar"
              }
            }
          }
        }
      } // inline

      def unwrap(list: Seq[AnyRef])(fn: PartialFunction[Command,Unit]) = list.toList match {
        case head :: Nil => head match {
          case c: Command => fn.isDefinedAt(c) match {
            case true => fn(c)
            case false => fail("Didn't find expected type in list: %s".format(c.getClass))
          }
          case _ => fail("Expected to find a command in the list")
        }
        case _ => fail("Expected single element list")
      }

      "unified requests" >> {
        "GET" >> {
          codec(wrap("*2\r\n")) mustEqual Nil
          codec(wrap("$3\r\n")) mustEqual Nil
          codec(wrap("GET\r\n")) mustEqual Nil
          codec(wrap("$3\r\n")) mustEqual Nil
          codec(wrap("bar\r\n")) mustEqual List(Get("bar"))
        }
        "MGET" >> {
          codec(wrap("*3\r\n")) mustEqual Nil
          codec(wrap("$4\r\n")) mustEqual Nil
          codec(wrap("MGET\r\n")) mustEqual Nil
          codec(wrap("$3\r\n")) mustEqual Nil
          codec(wrap("foo\r\n")) mustEqual Nil
          codec(wrap("$3\r\n")) mustEqual Nil
          codec(wrap("bar\r\n")) mustEqual List(MGet(List(foo, bar)))
        }
        "MSET" >> {
          codec(wrap("*5\r\n")) mustEqual Nil
          codec(wrap("$4\r\n")) mustEqual Nil
          codec(wrap("MSET\r\n")) mustEqual Nil
          codec(wrap("$3\r\n")) mustEqual Nil
          codec(wrap("foo\r\n")) mustEqual Nil
          codec(wrap("$7\r\n")) mustEqual Nil
          codec(wrap("bar baz\r\n")) mustEqual Nil
          codec(wrap("$3\r\n")) mustEqual Nil
          codec(wrap("bar\r\n")) mustEqual Nil
          codec(wrap("$5\r\n")) mustEqual Nil
          codec(wrap("Hello\r\n")) match {
            case MSet(kv) :: Nil =>
              val nkv = kv.map { case(k,v) => (BytesToString(k.array), BytesToString(v.array)) }
              nkv mustEqual Map(
                "foo" -> "bar baz",
                "bar" -> "Hello")
            case _ => fail("Expected MSet to be returned")
          }
        }
      }
    } // decode

    "Properly encode" >> {
      def unpackBuffer(buffer: ChannelBuffer) = {
        val bytes = new Array[Byte](buffer.readableBytes)
        buffer.readBytes(bytes)
        new String(bytes, "UTF-8")
      }

      "Inline Requests" >> {
        codec.send(Get("foo")) mustEqual List("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
      }

      "Unified Requests" >> {
        val value = "bar\r\nbaz"
        val valSz = 8
        val expected = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$%d\r\n%s\r\n".format(valSz, value)
        codec.send(Set(foo, s2cb(value))) mustEqual List(expected)
      }
    } // Encode properly

  } // A Redis Request

  "A Redis Response" should {
    val replyCodec = new ReplyCodec
    val (codec, counter) = TestCodec(replyCodec.decode, replyCodec.encode)

    "Properly decode" >> {
      "status replies" >> {
        codec(wrap("+OK\r\n")) mustEqual List(StatusReply("OK"))
        codec(wrap("+OK\r\n+Hello World\r\n")) mustEqual List(
          StatusReply("OK"),
          StatusReply("Hello World"))
        codec(wrap("+\r\n")) must throwA[ServerError]
      }
      "error replies" >> {
        codec(wrap("-BAD\r\n")) mustEqual List(ErrorReply("BAD"))
        codec(wrap("-BAD\r\n-Bad Thing\r\n")) mustEqual List(
          ErrorReply("BAD"),
          ErrorReply("Bad Thing"))
        codec(wrap("-\r\n")) must throwA[ServerError]
      }
      "integer replies" >> {
        codec(wrap(":-2147483648\r\n")) mustEqual List(IntegerReply(-2147483648))
        codec(wrap(":0\r\n")) mustEqual List(IntegerReply(0))
        codec(wrap(":2147483647\r\n")) mustEqual List(IntegerReply(2147483647))
        codec(wrap(":2147483648\r\n")) mustEqual List(IntegerReply(2147483648L))
        codec(wrap(":9223372036854775807\r\n")) mustEqual List(IntegerReply(9223372036854775807L))
        codec(wrap(":9223372036854775808\r\n")) must throwA[ServerError]
        codec(wrap(":-9223372036854775807\r\n")) mustEqual List(IntegerReply(-9223372036854775807L))
        codec(wrap(":-9223372036854775809\r\n")) must throwA[ServerError]
        codec(wrap(":-2147483649\r\n")) mustEqual List(IntegerReply(-2147483649L))
        codec(wrap(":1\r\n:2\r\n")) mustEqual List(IntegerReply(1), IntegerReply(2))
        codec(wrap(":\r\n")) must throwA[ServerError]
      }
      "bulk replies" >> {
        codec(wrap("$3\r\nfoo\r\n")) match {
          case reply :: Nil => reply match {
            case BulkReply(msg) => CBToString(msg) mustEqual "foo"
            case _ => fail("Expected BulkReply, got something else")
          }
          case _ => fail("Found no or multiple reply lines")
        }
        codec(wrap("$8\r\nfoo\r\nbar\r\n")) match {
          case reply :: Nil => reply match {
            case BulkReply(msg) => CBToString(msg) mustEqual "foo\r\nbar"
            case _ => fail("Expected BulkReply, got something else")
          }
          case _ => fail("Found no or multiple reply lines")
        }
        codec(wrap("$3\r\nfoo\r\n$3\r\nbar\r\n")) match {
          case fooR :: barR :: Nil =>
            fooR match {
              case BulkReply(msg) => CBToString(msg) mustEqual "foo"
              case _ => fail("Expected BulkReply")
            }
            barR match {
              case BulkReply(msg) => CBToString(msg) mustEqual "bar"
              case _ => fail("Expected BulkReply")
            }
          case _ => fail("Expected two elements in list")
        }
        codec(wrap("$-1\r\n")) match {
          case reply :: Nil => reply must haveClass[EmptyBulkReply]
          case _ => fail("Invalid reply for empty bulk reply")
        }
      }
      "empty multi-bulk replies" >> {
        codec(wrap("*0\r\n")) mustEqual List(EmptyMBulkReply())
      }
      "multi-bulk replies" >> {
        codec(wrap("*4\r\n")) mustEqual Nil
        codec(wrap("$3\r\n")) mustEqual Nil
        codec(wrap("foo\r\n")) mustEqual Nil
        codec(wrap("$3\r\n")) mustEqual Nil
        codec(wrap("bar\r\n")) mustEqual Nil
        codec(wrap("$5\r\n")) mustEqual Nil
        codec(wrap("Hello\r\n")) mustEqual Nil
        codec(wrap("$5\r\n")) mustEqual Nil
        codec(wrap("World\r\n")) match {
          case reply :: Nil => reply match {
            case MBulkReply(msgs) =>
              ReplyFormat.toString(msgs) mustEqual List("foo","bar","Hello","World")
            case _ => fail("Expected MBulkReply")
          }
          case _ => fail("Expected one element in list")
        }

        codec(wrap("*3\r\n")) mustEqual Nil
        codec(wrap("$3\r\n")) mustEqual Nil
        codec(wrap("foo\r\n")) mustEqual Nil
        codec(wrap("$-1\r\n")) mustEqual Nil
        codec(wrap("$3\r\n")) mustEqual Nil
        codec(wrap("bar\r\n")) match {
          case reply :: Nil => reply match {
            case MBulkReply(msgs) =>
              ReplyFormat.toString(msgs) mustEqual List(
                "foo",
                "nil",
                "bar")
            case _ => fail("Expected MBulkReply")
          }
          case _ => fail("Expected one element in list")
        }

      }
    }

    "Properly encode" >> {
      def unpackBuffer(buffer: ChannelBuffer) = {
        val bytes = new Array[Byte](buffer.readableBytes)
        buffer.readBytes(bytes)
        new String(bytes, "UTF-8")
      }

      "Status Replies" >> {
        codec.send(StatusReply("OK")) mustEqual List("+OK\r\n")
      }
      "Error Replies" >> {
        codec.send(ErrorReply("BAD")) mustEqual List("-BAD\r\n")
      }
      "Integer Replies" >> {
        codec.send(IntegerReply(123)) mustEqual List(":123\r\n")
        codec.send(IntegerReply(-123)) mustEqual List(":-123\r\n")
      }
      "Bulk Replies" >> {
        val expected = "$8\r\nfoo\r\nbar\r\n"
        codec.send(BulkReply(StringToChannelBuffer("foo\r\nbar"))) mustEqual List(expected)
      }
      "Multi Bulk Replies" >> {
        val messages = List(BulkReply(StringToChannelBuffer("foo")),
          BulkReply(StringToChannelBuffer("bar")))
        val expected = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
        codec.send(MBulkReply(messages)) mustEqual List(expected)
      }
    } // Encode properly

  }
}
