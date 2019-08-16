package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.RedisRequestTest
import com.twitter.finagle.redis.tags.CodecTest
import com.twitter.io.Buf
import org.scalacheck.Gen

final class SortedSetCodecSuite extends RedisRequestTest {

  test("ZADD", CodecTest) {
    forAll(Gen.nonEmptyListOf(genZMember)) { ms =>
      assert(
        encodeCommand(ZAdd(Buf.Utf8("foo"), ms)) ==
          Seq("ZADD", "foo") ++ ms.flatMap(zm => Seq(zm.score.toString, zm.member.asString))
      )
    }
  }

  test("ZCARD", CodecTest) { checkSingleKey("ZCARD", ZCard.apply) }

  test("INCRBY", CodecTest) {
    forAll { (k: Buf, m: Buf) =>
      assert(encodeCommand(ZIncrBy(k, 0.42, m)) == Seq("ZINCRBY", k.asString, "0.42", m.asString))
    }
  }

  test("ZCOUNT", CodecTest) {
    forAll { (k: Buf, a: ZInterval, b: ZInterval) =>
      assert(encodeCommand(ZCount(k, a, b)) == Seq("ZCOUNT", k.asString, a.toString, b.toString))
    }
  }

  test("ZREM", CodecTest) { checkSingleKeyMultiVal("ZREM", ZRem.apply) }
  test("ZSCORE", CodecTest) { checkSingleKeySingleVal("ZSCORE", ZScore.apply) }
  test("ZRANK", CodecTest) { checkSingleKeySingleVal("ZRANK", ZRank.apply) }

  test("ZRANGE", CodecTest) {
    forAll { (k: Buf, a: Int, b: Int) =>
      assert(encodeCommand(ZRange(k, a, b)) == Seq("ZRANGE", k.asString, a.toString, b.toString))
      assert(
        encodeCommand(ZRange(k, a, b, WithScores)) ==
          Seq("ZRANGE", k.asString, a.toString, b.toString, "WITHSCORES")
      )
    }
  }

  test("ZREVRANK", CodecTest) { checkSingleKeySingleVal("ZREVRANK", ZRevRank.apply) }

  test("ZINTERSTORE|ZUNIONSTORE", CodecTest) {
    forAll(genBuf, Gen.nonEmptyListOf(genBuf)) { (d, keys) =>
      assert(
        encodeCommand(ZInterStore(d, keys)) ==
          Seq("ZINTERSTORE", d.asString, keys.length.toString) ++ keys.map(_.asString)
      )
      assert(
        encodeCommand(ZUnionStore(d, keys)) ==
          Seq("ZUNIONSTORE", d.asString, keys.length.toString) ++ keys.map(_.asString)
      )
    }
  }

  test("ZREVRANGE") {
    forAll { (k: Buf, a: Long, b: Long) =>
      assert(
        encodeCommand(ZRevRange(k, a, b)) == Seq("ZREVRANGE", k.asString, a.toString, b.toString)
      )
    }
  }

  test("ZREVRANGEBYSCORE") {
    checkSingleKey2ArbitraryVals(
      "ZREVRANGEBYSCORE",
      (k: Buf, a: ZInterval, b: ZInterval) => ZRevRangeByScore(k, a, b)
    )
  }

  test("ZREMRANGEBYSCORE") {
    checkSingleKey2ArbitraryVals(
      "ZREMRANGEBYSCORE",
      (k: Buf, a: ZInterval, b: ZInterval) => ZRemRangeByScore(k, a, b)
    )
  }

  test("ZREMRANGEBYRANK") {
    checkSingleKey2ArbitraryVals(
      "ZREMRANGEBYRANK",
      (k: Buf, a: Long, b: Long) => ZRemRangeByRank(k, a, b)
    )
  }

  test("ZPOPMIN", CodecTest) {
    checkSingleKeyOptionCount("ZPOPMIN", ZPopMin.apply)
  }

  test("ZPOPMAX", CodecTest) {
    checkSingleKeyOptionCount("ZPOPMAX", ZPopMax.apply)
  }
}
