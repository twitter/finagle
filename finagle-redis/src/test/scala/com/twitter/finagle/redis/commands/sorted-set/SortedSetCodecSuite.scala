package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.RedisRequestTest
import com.twitter.finagle.redis.tags.CodecTest
import com.twitter.io.Buf
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class SortedSetCodecSuite extends RedisRequestTest {

  test("ZADD", CodecTest) {
    forAll { ms: NelList[ZMember] =>
      assert(encode(ZAdd(Buf.Utf8("foo"), ms.list)) ==
        Seq("ZADD", "foo") ++ ms.list.flatMap(zm => Seq(zm.score.toString, zm.memberBuf.asString)))
    }
  }

  test("ZCARD", CodecTest) { checkSingleKey("ZCARD", ZCard.apply) }

  test("INCRBY", CodecTest) {
    forAll { (k: Buf, m: Buf) =>
      assert(encode(ZIncrBy(k, 0.42, m)) == Seq("ZINCRBY", k.asString, "0.42", m.asString))
    }
  }

  test("ZCOUNT", CodecTest) {
    forAll { (k: Buf, a: ZInterval, b: ZInterval) =>
      assert(encode(ZCount(k, a, b)) == Seq("ZCOUNT", k.asString, a.toString, b.toString))
    }
  }

  test("ZREM", CodecTest) { checkSingleKeyMultiVal("ZREM", ZRem.apply) }
  test("ZSCORE", CodecTest) { checkSingleKeySingleVal("ZSCORE", ZScore.apply) }
  test("ZRANK", CodecTest) { checkSingleKeySingleVal("ZRANK", ZRank.apply) }

  test("ZRANGE", CodecTest) {
    forAll { (k: Buf, a: Int, b: Int) =>
      assert(encode(ZRange(k, a, b)) == Seq("ZRANGE", k.asString, a.toString, b.toString))
      assert(encode(ZRange(k, a, b, WithScores)) ==
        Seq("ZRANGE", k.asString, a.toString, b.toString, "WITHSCORES"))
    }
  }

  test("ZREVRANK", CodecTest) { checkSingleKeySingleVal("ZREVRANK", ZRevRank.apply) }

  test("ZINTERSTORE|ZUNIONSTORE", CodecTest) {
    forAll { (d: Buf, keys: NelList[Buf]) =>
      assert(encode(ZInterStore(d, keys.list)) ==
        Seq("ZINTERSTORE", d.asString, keys.list.length.toString) ++ keys.list.map(_.asString))

      assert(encode(ZUnionStore(d, keys.list)) ==
        Seq("ZUNIONSTORE", d.asString, keys.list.length.toString) ++ keys.list.map(_.asString))
    }
  }

  test("ZREVRANGE") {
    forAll { (k: Buf, a: Long, b: Long) =>
      assert(encode(ZRevRange(k, a, b)) == Seq("ZREVRANGE", k.asString, a.toString, b.toString))
    }
  }

  test("ZREVRANGEBYSCORE") {
    checkSingleKey2ArbitraryVals("ZREVRANGEBYSCORE",
      (k: Buf, a: ZInterval, b: ZInterval) => ZRevRangeByScore(k, a, b))
  }

  test("ZREMRANGEBYSCORE") {
    checkSingleKey2ArbitraryVals("ZREMRANGEBYSCORE",
      (k: Buf, a: ZInterval, b: ZInterval) => ZRemRangeByScore(k, a, b))
  }

  test("ZREMRANGEBYRANK") {
    checkSingleKey2ArbitraryVals("ZREMRANGEBYRANK",
      (k: Buf, a: Long, b: Long) => ZRemRangeByRank(k, a, b))
  }
}
