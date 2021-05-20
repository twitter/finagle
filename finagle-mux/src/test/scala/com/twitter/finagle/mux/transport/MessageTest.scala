package com.twitter.finagle.mux.transport

import com.twitter.finagle.{Dentry, Dtab, Failure, Path, tracing}
import com.twitter.io.Buf
import com.twitter.util.Time
import com.twitter.conversions.DurationOps._
import scala.collection.mutable
import org.scalatest.funsuite.AnyFunSuite

class MessageTest extends AnyFunSuite {
  import Message._

  def buf(n: Int) = Buf.ByteArray.Owned((0 until n).toArray.map(_.toByte))
  val body = buf(4)

  val goodTags = Seq(8388607, 1, 123)
  val goodVersions = Seq(100: Short, 200: Short, 300: Short)
  val goodTraceIds = Seq(None, Some(tracing.Trace.nextId))
  val goodBufs = Seq(Buf.Empty, buf(1), buf(4), buf(100))
  val goodStrings = Seq("", "Hello, world!", "☺☹")
  val goodKeys = goodStrings.map(Buf.Utf8(_))

  val goodDentries = Seq("/a=>/b", "/foo=>/$/inet/twitter.com/80") map (Dentry.read)
  val goodDtabs = goodDentries.permutations map { ds => Dtab(ds.toIndexedSeq) }
  val goodDests = Seq("/", "/okay", "/foo/bar/baz") map (Path.read)
  val goodDurationLeases = Seq(Message.Tlease.MinLease, Message.Tlease.MaxLease)
  val goodTimeLeases = Seq(Time.epoch, Time.now, Time.now + 5.minutes)
  val goodContexts =
    Seq() ++ (for { k <- goodKeys; v <- goodBufs } yield (k, v)).combinations(2).toSeq

  test("d(e(m)) == m") {
    val ms = mutable.Buffer[Message]()

    ms ++= (for {
      tag <- goodTags
      version <- goodVersions
      ctx <- goodContexts
    } yield Tinit(tag, version, ctx))

    ms ++= (for {
      tag <- goodTags
      version <- goodVersions
      ctx <- goodContexts
    } yield Rinit(tag, version, ctx))

    ms ++= (for {
      tag <- goodTags
      traceId <- goodTraceIds
      body <- goodBufs
    } yield Treq(tag, traceId, body))

    ms ++= (for {
      tag <- goodTags
      body <- goodBufs
    } yield RreqOk(tag, body))

    ms ++= (for {
      tag <- goodTags
    } yield Tdrain(tag))

    ms ++= (for {
      tag <- goodTags
      reason <- goodStrings
    } yield Tdiscarded(tag, reason))

    ms ++= (for {
      tag <- goodTags
      ctx <- goodContexts
      dest <- goodDests
      dtab <- goodDtabs
      body <- goodBufs
    } yield Tdispatch(tag, ctx, dest, dtab, body))

    ms ++= (for {
      tag <- goodTags
      ctx <- goodContexts
      body <- goodBufs
    } yield RdispatchOk(tag, ctx, body))

    ms ++= (for {
      tag <- goodTags
      ctx <- goodContexts
      err <- goodStrings
    } yield RdispatchError(tag, ctx, err))

    ms ++= (for {
      tag <- goodTags
      ctx <- goodContexts
    } yield RdispatchNack(tag, ctx))

    ms ++= (for {
      lease <- goodDurationLeases
    } yield Tlease(lease))

    ms ++= (for {
      lease <- goodTimeLeases
    } yield Tlease(lease))

    def assertEquiv(a: Message, b: Message) = (a, b) match {
      case (Tdispatch(tag1, ctxs1, dst1, dtab1, req1), Tdispatch(tag2, ctxs2, dst2, dtab2, req2)) =>
        assert(
          tag1 == tag2 && ctxs1 == ctxs2 && dst1 == dst2 &&
            Equiv[Dtab].equiv(dtab1, dtab2) && req1 == req2
        )
      case (a, b) => assert(a == b)
    }

    // Debugging tip: in an error message, 'm' is the RHS.
    for (m <- ms)
      assertEquiv(decode(encode(m)), m)
  }

  test("not encode invalid messages") {
    assert(intercept[Failure] {
      encode(Treq(-1, Some(tracing.Trace.nextId), body))
    } == Failure.wrap(BadMessageException("invalid tag number -1")))
    assert(intercept[Failure] {
      encode(Treq(1 << 24, Some(tracing.Trace.nextId), body))
    } == Failure.wrap(BadMessageException("invalid tag number 16777216")))
  }

  test("not decode invalid messages") {
    val short = intercept[Failure] { decode(Buf.Empty) }
    assert(short.why.startsWith("short message"))
    assert(short.cause.get.isInstanceOf[BadMessageException])

    assert(
      intercept[Failure] {
        decode(Buf.ByteArray.Owned(Array[Byte](0, 0, 0, 1)))
      } == Failure.wrap(
        BadMessageException(
          "unknown message type: 0 [tag=1]. Payload bytes: 0. First 0 bytes of the payload: ''"
        )
      )
    )
    assert(
      intercept[Failure] {
        decode(Buf.ByteArray.Owned(Array[Byte](0, 0, 0, 1, 0x01, 0x02, 0x0e, 0x0f)))
      } == Failure.wrap(
        BadMessageException(
          "unknown message type: 0 [tag=1]. Payload bytes: 4. First 4 bytes of the payload: '01020e0f'"
        )
      )
    )
    assert(
      intercept[Failure] {
        decode(Buf.ByteArray.Owned(Array[Byte](0, 0, 0, 1) ++ Seq.fill(32)(1.toByte).toArray[Byte]))
      } == Failure.wrap(
        BadMessageException(
          "unknown message type: 0 [tag=1]. Payload bytes: 32. First 16 bytes of the payload: '01010101010101010101010101010101'"
        )
      )
    )
  }

  test("decode fragments") {
    val msgs = Seq(
      Tdispatch(
        Message.Tags.setMsb(goodTags.head),
        goodContexts.head,
        goodDests.head,
        Dtab.empty,
        goodBufs.head
      ),
      RdispatchOk(Message.Tags.setMsb(goodTags.last), goodContexts.last, goodBufs.last)
    )

    for (m <- msgs) {
      assert(decode(encode(m)) == Fragment(m.typ, m.tag, m.buf))
    }
  }

  test("extract control messages") {
    val tag = 0
    val buf = Buf.Empty

    assert(ControlMessage.unapply(Treq(tag, None, buf)) == None)
    assert(ControlMessage.unapply(RreqOk(0, buf)) == None)
    assert(ControlMessage.unapply(Tdispatch(tag, Seq.empty, Path.empty, Dtab.empty, buf)) == None)
    assert(ControlMessage.unapply(RdispatchOk(tag, Seq.empty, buf)) == None)

    assert(ControlMessage.unapply(Tdrain(tag)) == Some(tag))
    assert(ControlMessage.unapply(Rdrain(tag)) == Some(tag))
    assert(ControlMessage.unapply(Tping(tag)) == Some(tag))
    assert(ControlMessage.unapply(Rping(tag)) == Some(tag))
    assert(ControlMessage.unapply(Tdiscarded(tag, "")) == Some(tag))
    assert(ControlMessage.unapply(Tlease(0, 0L)) == Some(tag))
  }

  test("context entries are backed by Buf.Empty or an exact sized ByteArray") {
    def checkBuf(buf: Buf): Unit = buf match {
      case Buf.Empty => assert(true) // ok
      case Buf.ByteArray.Owned(array, 0, end) => assert(end == array.length)
      case msg => fail(s"Unexpected Buf: $msg")
    }

    val msg = RdispatchOk(0, goodContexts.flatten, Buf.Empty)
    val RdispatchOk(0, ctxs, Buf.Empty) = decode(Buf.ByteArray.coerce(encode(msg)))
    ctxs.foreach {
      case (k, v) =>
        checkBuf(k)
        checkBuf(v)
    }
  }

  test("Message.encode(Message.PreEncoded)") {
    val preEncodedMessages = Seq(
      PreEncoded.Rping,
      PreEncoded.Tping
    )

    preEncodedMessages.foreach { msg: PreEncoded =>
      assert(Message.encode(msg.underlying) == msg.encodedBuf)
      assert(msg.encodedBuf eq Message.encode(msg))
      assert(msg.toString == msg.underlying.toString)
    }
  }
}
