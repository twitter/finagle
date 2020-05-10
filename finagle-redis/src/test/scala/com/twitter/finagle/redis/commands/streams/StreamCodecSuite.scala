package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.{ClientError, RedisRequestTest}
import com.twitter.io.Buf
import org.scalacheck.{Arbitrary, Gen}

final class StreamCodecSuite extends RedisRequestTest {
  test("XADD") {
    forAll { (key: Buf, a: Option[Buf], b: Map[Buf, Buf]) =>
      lazy val command = XAdd(key, a, b)

      if (a.exists(_.isEmpty) || b.isEmpty) {
        intercept[ClientError](encodeCommand(command))
      } else {
        val fvStr = b.toSeq.flatMap(t => Seq(t._1, t._2)).map(_.asString)
        assert(
          encodeCommand(command) == "XADD" +: (Seq(
            key.asString,
            a.map(_.asString).getOrElse("*")) ++ fvStr)
        )
      }
    }

    for {
      aa <- Arbitrary.arbitrary[Option[Buf]].sample
      bb <- Arbitrary.arbitrary[Map[Buf, Buf]].sample
    } {
      intercept[ClientError](encodeCommand(XAdd(Buf.Empty, aa, bb)))
    }
  }

  test("XTRIM") {
    forAll { (key: Buf, a: Long, b: Boolean) =>
      lazy val command = XTrim(key, a, b)
      if (a < 0) {
        intercept[ClientError](command)
      } else {
        assert(
          encodeCommand(command) == "XTRIM" +: (Seq(key.asString, "MAXLEN") ++ (if (!b) Seq("~")
                                                                                else
                                                                                  Seq.empty) ++ Seq(
            a.toString
          ))
        )
      }

      for {
        aa <- Arbitrary.arbitrary[Long].sample
        bb <- Arbitrary.arbitrary[Boolean].sample
      } {
        intercept[ClientError](encodeCommand(XTrim(Buf.Empty, aa, bb)))
      }
    }
  }

  test("XDEL") {
    checkSingleKeyMultiVal("XDEL", XDel.apply)
  }

  test("XRANGE") {
    forAll { (key: Buf, start: Buf, end: Buf, count: Option[Long]) =>
      val countPart = count.map(c => Seq("COUNT", c.toString)).getOrElse(Seq.empty)
      assert(
        encodeCommand(XRange(key, start, end, count)) == "XRANGE" +: (Seq(key, start, end)
          .map(_.asString) ++ countPart)
      )
    }
  }

  test("XREVRANGE") {
    forAll { (key: Buf, start: Buf, end: Buf, count: Option[Long]) =>
      val countPart = count.map(c => Seq("COUNT", c.toString)).getOrElse(Seq.empty)
      assert(
        encodeCommand(XRevRange(key, start, end, count)) == "XREVRANGE" +: (Seq(key, start, end)
          .map(_.asString) ++ countPart)
      )
    }
  }

  test("XLEN") {
    checkSingleKey("XLEN", XLen.apply)
  }

  test("XREAD") {
    forAll(Gen.nonEmptyListOf(genBuf), Gen.nonEmptyListOf(genBuf)) {
      (keys: Seq[Buf], values: Seq[Buf]) =>
        if (keys.size != values.size) {
          intercept[ClientError](XRead(None, None, keys, values))
        } else {
          assert(
            encodeCommand(XRead(None, None, keys, values)) == "XREAD" +: "STREAMS" +: (keys
              .map(_.asString) ++ values.map(_.asString))
          )
        }
    }

    for {
      c <- Arbitrary.arbitrary[Option[Long]].sample
      ms <- Arbitrary.arbitrary[Option[Long]].sample
      b <- Gen.nonEmptyListOf(genBuf).sample
    } {
      intercept[ClientError](XRead(c, ms, Seq.empty, b))
      intercept[ClientError](XRead(c, ms, b, Seq.empty))
    }
  }

  test("XREADGROUP") {
    forAll(genBuf, genBuf, Gen.nonEmptyListOf(genBuf), Gen.nonEmptyListOf(genBuf)) {
      (group: Buf, consumer: Buf, keys: Seq[Buf], values: Seq[Buf]) =>
        if (keys.size != values.size) {
          intercept[ClientError](XReadGroup(group, consumer, None, None, keys, values))
        } else {
          val expected = ("XREADGROUP" +: "GROUP" +: Seq(group, consumer).map(_.asString)) ++ (Seq(
            "STREAMS"
          ) ++ (keys.map(_.asString) ++ values.map(_.asString)))
          assert(encodeCommand(XReadGroup(group, consumer, None, None, keys, values)) == expected)
        }
    }

    for {
      a <- genBuf.sample
      c <- Arbitrary.arbitrary[Option[Long]].sample
      ms <- Arbitrary.arbitrary[Option[Long]].sample
      b <- Gen.nonEmptyListOf(genBuf).sample
    } {
      intercept[ClientError](XReadGroup(a, a, c, ms, Seq.empty, b))
      intercept[ClientError](XReadGroup(a, a, c, ms, b, Seq.empty))
      intercept[ClientError](XReadGroup(Buf.Empty, a, c, ms, b, Seq.empty))
      intercept[ClientError](XReadGroup(a, Buf.Empty, c, ms, b, Seq.empty))
    }
  }

  test("XGROUP CREATE") {
    checkSingleKeyDoubleVal("XGROUP CREATE", XGroupCreate.apply)
  }

  test("XGROUP SETID") {
    checkSingleKeySingleVal("XGROUP SETID", XGroupSetId.apply)
  }

  test("XGROUP DESTROY") {
    checkSingleKeySingleVal("XGROUP DESTROY", XGroupDestroy.apply)
  }

  test("XGROUP DELCONSUMER") {
    checkSingleKeyDoubleVal("XGROUP DELCONSUMER", XGroupDelConsumer.apply)
  }

  test("XACK") {
    checkSingleKeyDoubleVal("XACK", (a, b, c) => XAck.apply(a, b, Seq(c)))
  }

  test("XPENDING") {
    checkSingleKeySingleVal("XPENDING", XPending.apply)
  }

  test("XPENDING range options") {
    forAll(Gen.listOfN(4, genBuf), Gen.choose(0, Long.MaxValue)) {
      case (List(key, group, start, end), count) =>
        val expected = "XPENDING" +: (Seq(key, group, start, end).map(_.asString) ++ Seq(
          count.toString
        ))
        assert(encodeCommand(XPendingRange(key, group, start, end, count, None)) == expected)
    }

    forAll(Gen.listOfN(3, genBuf), Gen.choose(0, Long.MaxValue)) {
      case (List(a, b, c), count) =>
        intercept[ClientError](XPendingRange(a, b, c, Buf.Empty, count, None))
        intercept[ClientError](XPendingRange(a, b, Buf.Empty, c, count, None))
        intercept[ClientError](XPendingRange(a, Buf.Empty, b, c, count, None))
        intercept[ClientError](XPendingRange(Buf.Empty, a, b, c, count, None))
    }
  }

  test("XCLAIM") {
    val genLong = Gen.choose(0, Long.MaxValue)
    val genTime = Gen.option(Gen.oneOf(genLong.map(XClaimMillis), genLong.map(XClaimUnixTs)))
    val genBool = Gen.oneOf(0, 1).map(_ == 1)
    forAll(
      Gen.listOfN(3, genBuf),
      genLong,
      Gen.nonEmptyListOf(genBuf),
      Gen.option(genLong),
      genTime,
      genBool
    ) {
      case (List(key, group, consumer), minIdle, ids, retry, time, b) =>
        val flags = if (b) Seq("FORCE", "JUSTID") else Seq()
        val await = time
          .map {
            case XClaimMillis(t) => Seq("IDLE", t.toString)
            case XClaimUnixTs(t) => Seq("TIME", t.toString)
          }.getOrElse(Seq.empty)
        val tries = retry.map(r => Seq("RETRYCOUNT", r.toString)).getOrElse(Seq.empty)

        val expected = "XCLAIM" +: (Seq(key, group, consumer).map(_.asString) ++ Seq(
          minIdle.toString
        ) ++ ids.map(_.asString) ++ await ++ tries ++ flags)

        assert(
          encodeCommand(XClaim(key, group, consumer, minIdle, ids, time, retry, b, b)) == expected
        )
    }

    forAll(Gen.listOfN(3, genBuf)) {
      case List(a, b, c) =>
        intercept[ClientError](XClaim(a, b, c, 0, Seq(), None, None, false, false))
        intercept[ClientError](XClaim(a, b, c, 0, Seq(Buf.Empty), None, None, false, false))
        intercept[ClientError](XClaim(a, b, Buf.Empty, 0, Seq(a), None, None, false, false))
        intercept[ClientError](XClaim(a, Buf.Empty, b, 0, Seq(a), None, None, false, false))
        intercept[ClientError](XClaim(Buf.Empty, a, b, 0, Seq(a), None, None, false, false))
    }
  }

  test("XINFO CONSUMERS") {
    checkSingleKeySingleVal("XINFO CONSUMERS", XInfoConsumers.apply)
  }

  test("XINFO GROUPS") {
    checkSingleKey("XINFO GROUPS", XInfoGroups.apply)
  }

  test("XINFO STREAM") {
    checkSingleKey("XINFO STREAM", XInfoStream.apply)
  }
}
