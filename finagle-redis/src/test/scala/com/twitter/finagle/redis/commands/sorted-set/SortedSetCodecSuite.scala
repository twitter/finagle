package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.naggati.RedisRequestTest
import com.twitter.finagle.redis.tags.CodecTest
import com.twitter.finagle.redis.util.{CBToString, BytesToString, StringToChannelBuffer}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class SortedSetCodecSuite extends RedisRequestTest {

  test("Throw a ClientError for ZADD with invalid arguments", CodecTest) {
    List(
      "ZADD",
      "ZADD foo",
      "ZADD foo 123",
      "ZADD foo BAD_SCORE bar",
      "ZADD foo 123 bar BAD_SCORE bar"
    ).foreach { e =>
      intercept[ClientError] {
        codec(wrap("%s\r\n".format(e)))
      }
    }
  }

  test("Correctly encode ZADD with one member") {
    val nums = StringToChannelBuffer("nums")

    unwrap(codec(wrap("ZADD nums 3.14159 pi\r\n"))) {
      case ZAdd(nums, members) =>
        unwrap(members) {
          case ZMember(3.14159, value) =>
            assert(BytesToString(value.array) == "pi")
        }
    }
  }

  test("Correctly encode ZADD with two members") {
    val nums = StringToChannelBuffer("nums")

    unwrap(codec(wrap("ZADD nums 3.14159 pi 2.71828 e\r\n"))) {
      case ZAdd(nums, members) =>
        members.toList match {
          case pi :: e :: Nil =>
            unwrap(List(pi)) {
              case ZMember(3.14159, value) =>
                assert(BytesToString(value.array) == "pi")
            }
            unwrap(List(e)) {
              case ZMember(2.71828, value) =>
                assert(BytesToString(value.array) == "e")
            }
          case _ => fail("Expected two elements in list")
        }
    }
  }

  test("Throw a ClientError for ZCARD with no arguments", CodecTest) {
    intercept[ClientError] {
      codec(wrap("ZCARD\r\n"))
    }
  }

  test("Correctly encode ZCARD") {
    unwrap(codec(wrap("ZCARD foo\r\n"))) {
      case ZCard(key) =>
        assert(BytesToString(key.array) == "foo")
    }
  }

  test("Throw a ClientError for ZCOUNT with invalid arguments", CodecTest) {
    List(
      "ZCOUNT",
      "ZCOUNT foo",
      "ZCOUNT foo 1",
      "ZCOUNT foo 1 bar",
      "ZCOUNT foo bar 1",
      "ZCOUNT foo -inf foo",
      "ZCOUNT foo 1 +info",
      "ZCOUNT foo )1 3",
      "ZCOUNT foo (1 n"
    ).foreach { e =>
      intercept[ClientError] {
        codec(wrap("%s\r\n".format(e)))
      }
    }
  }

  test("Correctly encode ZCOUNT") {
    Map(
      "foo -inf +inf" -> ZCount(StringToChannelBuffer("foo"), ZInterval.MIN, ZInterval.MAX),
      "foo (1.0 3.0" -> ZCount(StringToChannelBuffer("foo"), ZInterval.exclusive(1), ZInterval(3))
    ).foreach {
      case(s, v) =>
        unwrap(codec(wrap("ZCOUNT %s\r\n".format(s)))) {
          case c: Command =>
            assert(c == v)
        }
    }
  }

  test("Throw a ClientError for ZINCRBY with invalid arguments", CodecTest) {
    List(
      "ZINCRBY",
      "ZINCRBY key",
      "ZINCRBY key 1",
      "ZINCRBY key bad member"
    ).foreach { b =>
      intercept[ClientError] {
        codec(wrap("%s\r\n".format(b)))
      }
    }
  }

  test("Correctly encode ZINCRBY with an integer argument") {
    val key = StringToChannelBuffer("key")

    unwrap(codec(wrap("ZINCRBY key 2 one\r\n"))) {
      case ZIncrBy(key, 2, member) =>
        assert(BytesToString(member.array) == "one")
    }
  }

  test("Correctly encode ZINCRBY with a floating point argument") {
    val key = StringToChannelBuffer("key")

    unwrap(codec(wrap("ZINCRBY key 2.1 one\r\n"))) {
      case ZIncrBy(key, value, member) =>
        assert(value == 2.1)
        assert(BytesToString(member.array) == "one")
    }
  }

  test("Throw a ClientError for ZINTERSTORE and ZUNIONSTORE with invalid arguments", CodecTest) {
    val bad = List(
      "%s", "%s foo", "%s foo 1 a b",
      "%s foo 2 a b WEIGHTS 2", "%s foo 2 a b WEIGHTS 1",
      "%s foo 2 a b WEIGHTS 2 2 AGGREGATE foo",
      "%s foo 2 a b c WEIGHTS 2 2 2", "%s foo 1 b WEIGHTS 2 AGGREGATE",
      "%s foo 2 a b WEIGHTS 2 2 2", "%s foo 1 a WEIGHTS a",
      "%s foo 1 a WEIGHTS 2 WEIGHTS 3",
      "%s foo 1 a AGGREGATE SUM AGGREGATE MAX"
    )

    List("ZINTERSTORE", "ZUNIONSTORE").foreach { cmd =>
      bad.foreach { b =>
        intercept[ClientError] {
          codec(wrap("%s\r\n".format(b.format(cmd))))
        }
      }
    }
  }

  private def doCmd(cmd: String, rcmd: String) = codec(wrap(rcmd.format(cmd)))

  private def verifyIU(cmd: String, k: String, n: Int)(
    f: (Seq[ChannelBuffer], Option[Weights], Option[Aggregate]) => Unit
  ): PartialFunction[Command, Unit] = cmd match {
    case "ZINTERSTORE" => {
      case ZInterStore(k, n, keys, w, a) => f(keys, w, a)
    }
    case "ZUNIONSTORE" => {
      case ZUnionStore(k, n, keys, w, a) => f(keys, w, a)
    }
    case _ => throw new Exception("Unhandled type")
  }

  test("Correctly encode ZINTERSTORE and ZUNIONSTORE with two keys") {
    List("ZINTERSTORE", "ZUNIONSTORE").foreach { cmd =>
      unwrap(doCmd(cmd, "%s out 2 zset1 zset2\r\n")) {
        verifyIU(cmd, "out", 2) { (keys, weights, aggregate) =>
          assert(keys.map(s => BytesToString(s.array)) == List("zset1", "zset2"))
          assert(weights == None)
          assert(aggregate == None)
        }
      }
    }
  }

  test("Correctly encode ZINTERSTORE and ZUNIONSTORE with two keys and weights") {
    List("ZINTERSTORE", "ZUNIONSTORE").foreach { cmd =>
      unwrap(doCmd(cmd, "%s out 2 zset1 zset2 WEIGHTS 2 3\r\n")) {
        verifyIU(cmd, "out", 2) { (keys, weights, aggregate) =>
          assert(keys.map(s => BytesToString(s.array)) == List("zset1", "zset2"))
          assert(weights == Some(Weights(2, 3)))
          assert(aggregate == None)
        }
      }
    }
  }

  test("Correctly encode ZINTERSTORE and ZUNIONSTORE with two keys and aggregate sum") {
    List("ZINTERSTORE", "ZUNIONSTORE").foreach { cmd =>
      unwrap(doCmd(cmd, "%s out 2 zset1 zset2 aggregate sum\r\n")) {
        verifyIU(cmd, "out", 2) { (keys, weights, aggregate) =>
          assert(keys.map(s => BytesToString(s.array)) == List("zset1", "zset2"))
          assert(weights == None)
          assert(aggregate == Some(Aggregate.Sum))
        }
      }
    }
  }

  test("Correctly encode ZINTERSTORE and ZUNIONSTORE with two keys, weights, and aggregate min") {
    List("ZINTERSTORE", "ZUNIONSTORE").foreach { cmd =>
      unwrap(doCmd(cmd, "%s out 2 zset1 zset2 weights 2 3 aggregate min\r\n")) {
        verifyIU(cmd, "out", 2) { (keys, weights, aggregate) =>
          assert(keys.map(s => BytesToString(s.array)) == List("zset1", "zset2"))
          assert(weights == Some(Weights(2, 3)))
          assert(aggregate == Some(Aggregate.Min))
        }
      }
    }
  }

  test("Correctly encode ZINTERSTORE and ZUNIONSTORE with two keys, weights, and aggregate max") {
    List("ZINTERSTORE", "ZUNIONSTORE").foreach { cmd =>
      unwrap(doCmd(cmd, "%s out 2 zset1 zset2 aggregate max weights 2 3\r\n")) {
        verifyIU(cmd, "out", 2) { (keys, weights, aggregate) =>
          assert(keys.map(s => BytesToString(s.array)) == List("zset1", "zset2"))
          assert(weights == Some(Weights(2, 3)))
          assert(aggregate == Some(Aggregate.Max))
        }
      }
    }
  }

  test("Throw a ClientError for ZRANGE and ZREVRANGE with invalid arguments", CodecTest) {
    val bad = List(
      "%s", "%s myset", "%s myset 1", "%s myset 1 foo",
      "%s myset foo 1", "%s myset 0 2 blah"
    )

    List("ZRANGE", "ZREVRANGE").foreach { cmd =>
      bad.foreach { b =>
        intercept[ClientError] {
          codec(wrap("%s\r\n".format(b.format(cmd))))
        }
      }
    }
  }

  def verifyRange(
    cmd: String, k: String, start: Int, stop: Int, scored: Option[CommandArgument]
  ): PartialFunction[Command,Unit] = {
    cmd match {
      case "ZRANGE" => {
        case ZRange(k, start, stop, scored) => ()
      }
      case "ZREVRANGE" => {
        case ZRevRange(k, start, stop, scored) => ()
      }
    }
  }

  test("Correctly encode ZRANGE and ZREVRANGE without scores") {
    List("ZRANGE", "ZREVRANGE").foreach { cmd =>
      unwrap(doCmd(cmd, "%s myset 0 -1\r\n")) {
        verifyRange(cmd, "myset", 0, -1, None)
      }
    }
  }

  test("Correctly encode ZRANGE and ZREVRANGE with scores") {
    List("ZRANGE", "ZREVRANGE").foreach { cmd =>
      unwrap(doCmd(cmd, "%s myset 0 -1 withscores\r\n")) {
        verifyRange(cmd, "myset", 0, -1, Some(WithScores))
      }

      unwrap(doCmd(cmd, "%s myset 0 -1 WITHSCORES\r\n")) {
        verifyRange(cmd, "myset", 0, -1, Some(WithScores))
      }
    }
  }

  private def testZRangeBytes(key: Array[Byte]) = {
    val zrange = ZRange.get(ChannelBuffers.wrappedBuffer(key), 0, 1, None)
    val keyBack = zrange match {
      case ZRange(key, start, stop, withScores) => key
    }
    val bytesBack = keyBack.toByteBuffer.array
    assert(key.toSeq == bytesBack.toSeq)
  }

  test("Ensure that, after encoding ZRANGE, the bytes we threw in are the bytes we get back") {
    val goodKey = Array[Byte](58, 49, 127)
    val nonAsciiKey = Array[Byte](58, 49, -128)
    testZRangeBytes(goodKey)
    testZRangeBytes(nonAsciiKey)
  }

  test(
    "Throw a ClientError for ZRANGEBYSCORE and ZREVRANGEBYSCORE with invalid arguments",
    CodecTest
  ) {
    val bad = List(
      "%s", "%s key", "%s key -inf", "%s key -inf foo", "%s key foo +inf",
      "%s key 0 1 NOSCORES", "%s key 0 1 LIMOT 1 2", "%s key 0 1 LIMIT foo 1",
      "%s key 0 1 LIMIT 1 foo", "%s key 0 1 WITHSCORES WITHSCORES",
      "%s key 0 1 LIMIT 0 1 LIMIT 0 1 WITHSCORES", "%s key 0 1 LIMIT 0 1 NOSCORES",
      "%s key 0 1 LIMIT 1", "%s key 0 1 LIMIT 0 1 WITHSCORES NOSCORES"
    )

    List("ZRANGEBYSCORE", "ZREVRANGEBYSCORE").foreach { cmd =>
      bad.foreach { b =>
        intercept[ClientError] {
          codec(wrap("%s\r\n".format(b.format(cmd))))
        }
      }
    }
  }

  def verifyRangeByScore(cmd: String, k: String, min: ZInterval, max: ZInterval)(
    f: (Option[CommandArgument], Option[Limit]) => Unit
  ): PartialFunction[Command, Unit] = cmd match {
    case "ZRANGEBYSCORE" => {
      case ZRangeByScore(k, min, max, s, l) => f(s, l)
    }
    case "ZREVRANGEBYSCORE" => {
      case ZRevRangeByScore(k, min, max, s, l) => f(s, l)
    }
  }

  test("Correctly encode ZRANGEBYSCORE and ZREVRANGEBYSCORE from -inf to +inf") {
    List("ZRANGEBYSCORE", "ZREVRANGEBYSCORE").foreach { cmd =>
      unwrap(doCmd(cmd, "%s myzset -inf +inf\r\n")) {
        verifyRangeByScore(cmd, "myzset", ZInterval.MIN, ZInterval.MAX) { (s, l) =>
          assert(s == None)
          assert(l == None)
        }
      }
    }
  }

  test("Correctly encode ZRANGEBYSCORE and ZREVRANGEBYSCORE from 1 to 2") {
    List("ZRANGEBYSCORE", "ZREVRANGEBYSCORE").foreach { cmd =>
      unwrap(doCmd(cmd, "%s myzset 1 2\r\n")) {
        verifyRangeByScore(cmd, "myzset", ZInterval(1), ZInterval(2)) { (s, l) =>
          assert(s == None)
          assert(l == None)
        }
      }
    }
  }

  test("Correctly encode ZRANGEBYSCORE and ZREVRANGEBYSCORE from 1 (exclusive) to 2") {
    List("ZRANGEBYSCORE", "ZREVRANGEBYSCORE").foreach { cmd =>
      unwrap(doCmd(cmd, "%s myzset (1 2\r\n")) {
        verifyRangeByScore(cmd, "myzset", ZInterval.exclusive(1), ZInterval(2)) { (s, l) =>
          assert(s == None)
          assert(l == None)
        }
      }
    }
  }

  test("Correctly encode ZRANGEBYSCORE and ZREVRANGEBYSCORE from 1 (excl.) to 2 (excl.)") {
    List("ZRANGEBYSCORE", "ZREVRANGEBYSCORE").foreach { cmd =>
      unwrap(doCmd(cmd, "%s myzset (1 (2\r\n")) {
        verifyRangeByScore(cmd, "myzset", ZInterval.exclusive(1), ZInterval.exclusive(2)) {
          (s, l) =>
            assert(s == None)
            assert(l == None)
        }
      }
    }
  }

  test("Correctly encode ZRANGEBYSCORE and ZREVRANGEBYSCORE with limit") {
    List("ZRANGEBYSCORE", "ZREVRANGEBYSCORE").foreach { cmd =>
      unwrap(doCmd(cmd, "%s myzset -inf +inf LIMIT 1 5\r\n")) {
        verifyRangeByScore(cmd, "myzset", ZInterval.MIN, ZInterval.MAX) { (s, l) =>
          assert(s == None)
          assert(l == Some(Limit(1, 5)))
        }
      }
    }
  }

  test("Correctly encode ZRANGEBYSCORE and ZREVRANGEBYSCORE with limit and scores") {
    List("ZRANGEBYSCORE", "ZREVRANGEBYSCORE").foreach { cmd =>
      unwrap(doCmd(cmd, "%s myzset -inf +inf LIMIT 3 9 WITHSCORES\r\n")) {
        verifyRangeByScore(cmd, "myzset", ZInterval.MIN, ZInterval.MAX) { (s, l) =>
          assert(s == Some(WithScores))
          assert(l == Some(Limit(3, 9)))
        }
      }
    }
  }

  test("Throw a ClientError for ZRANK and ZREVRANK with invalid arguments", CodecTest) {
    val bad = List("%s", "%s key", "%s key member member")

    List("ZRANK", "ZREVRANK").foreach { cmd =>
      bad.foreach { b =>
        intercept[ClientError] {
          codec(wrap("%s\r\n".format(b.format(cmd))))
        }
      }
    }
  }

  def verifyRank(cmd: String, k: String)(f: ChannelBuffer => Unit): PartialFunction[Command, Unit] =
    cmd match {
      case "ZRANK" => {
        case ZRank(k, m) => f(m)
      }
      case "ZREVRANK" => {
        case ZRevRank(k, m) => f(m)
      }
    }

  test("Correctly encode ZRANK and ZREVRANK") {
    List("ZRANK", "ZREVRANK").foreach { cmd =>
      unwrap(doCmd(cmd, "%s myzset three\r\n")) {
        verifyRank(cmd, "myzset") { m =>
          assert(BytesToString(m.array) == "three")
        }
      }

      unwrap(doCmd(cmd, "%s myzset four\r\n")) {
        verifyRank(cmd, "myzset") { m =>
          assert(BytesToString(m.array) == "four")
        }
      }
    }
  }

  test("Throw a ClientError for ZREM with invalid arguments", CodecTest) {
    List("ZREM", "ZREM key").foreach { b =>
      intercept[ClientError] {
        codec(wrap("%s\r\n".format(b)))
      }
    }
  }

  test("Correctly encode ZREM with one member") {
    val key = StringToChannelBuffer("key")
    unwrap(codec(wrap("ZREM key member1\r\n"))) {
      case ZRem(key, members) =>
        assert(CBToString.fromList(members) == List("member1"))
    }
  }

  test("Correctly encode ZREM with two members") {
    val key = StringToChannelBuffer("key")
    unwrap(codec(wrap("ZREM key member1 member2\r\n"))) {
      case ZRem(key, members) =>
        assert(CBToString.fromList(members) == List("member1", "member2"))
    }
  }

  test("Throw a ClientError for ZREMRANGEBYRANK with invalid arguments", CodecTest) {
    List("%s", "%s key", "%s key start", "%s key 1", "%s key 1 stop", "%s key start 2").foreach {
      b =>
        intercept[ClientError] {
          codec(wrap("%s\r\n".format(b.format("ZREMRANGEBYRANK"))))
        }
    }
  }

  test("Correctly encode ZREMRANGEBYRANK") {
    val key = StringToChannelBuffer("key")
    unwrap(codec(wrap("ZREMRANGEBYRANK key 0 1\r\n"))) {
      case ZRemRangeByRank(key, start, stop) =>
        assert(start == 0)
        assert(stop == 1)
    }
  }

  test("Throw a ClientError for ZREMRANGEBYSCORE with invalid arguments", CodecTest) {
    List("%s", "%s key", "%s key min", "%s key min max", "%s key ( 1", "%s key (1 max").foreach {
      b =>
        intercept[ClientError] {
          codec(wrap("%s\r\n".format(b.format("ZREMRANGEBYSCORE"))))
        }
    }
  }

  test("Correctly encode ZREMRANGEBYSCORE") {
    val key = StringToChannelBuffer("key")
    unwrap(codec(wrap("ZREMRANGEBYSCORE key -inf (2.0\r\n"))) {
      case ZRemRangeByScore(key, min, max) =>
        assert(min == ZInterval.MIN)
        assert(max == ZInterval.exclusive(2))
    }
  }

  test("Throw a ClientError for ZSCORE with invalid arguments", CodecTest) {
    List("ZSCORE","ZSCORE key").foreach { b =>
      intercept[ClientError] {
        codec(wrap("%s\r\n".format(b)))
      }
    }
  }

  test("Correctly encode ZSCORE") {
    val myset = StringToChannelBuffer("myset")
    unwrap(codec(wrap("ZSCORE myset one\r\n"))) {
      case ZScore(myset, one) =>
        assert(BytesToString(one.array) == "one")
    }
  }
}
