package com.twitter.finagle.mux.transport

import com.twitter.finagle.{Path, tracing, Dtab, Dentry}
import com.twitter.io.Charsets
import com.twitter.util.Time
import com.twitter.util.TimeConversions.intToTimeableNumber
import org.jboss.netty.buffer.ChannelBuffers
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}
import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class MessageTest extends FunSuite with AssertionsForJUnit {
  import Message._

  def buf(n: Int) = ChannelBuffers.wrappedBuffer((0 until n).toArray.map(_.toByte))
  val body = buf(4)

  val goodTags = Seq(8388607, 1, 123)
  val goodTraceIds = Seq(None, Some(tracing.Trace.nextId))
  val goodBufs = Seq(ChannelBuffers.EMPTY_BUFFER, buf(1), buf(4), buf(100))
  val goodStrings = Seq("", "Hello, world!", "☺☹")
  val goodKeys = goodStrings map { s =>
    val bytes = s.getBytes(Charsets.Utf8)
    ChannelBuffers.wrappedBuffer(bytes)
  }
  val goodDentries = Seq("/a=>/b", "/foo=>/$/inet/twitter.com/80") map(Dentry.read)
  val goodDtabs = goodDentries.permutations map { ds => Dtab(ds.toIndexedSeq) }
  val goodDests = Seq("/", "/okay", "/foo/bar/baz") map(Path.read)
  val goodDurationLeases = Seq(Message.Tlease.MinLease, Message.Tlease.MaxLease)
  val goodTimeLeases = Seq(Time.epoch, Time.now, Time.now + 5.minutes)
  val goodContexts =
    Seq() ++ (for { k <- goodKeys; v <- goodBufs } yield (k, v)).combinations(2).toSeq

  test("d(e(m)) == m") {
    val ms = mutable.Buffer[Message]()

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
      case (Tdispatch(tag1, ctxs1, dst1, dtab1, req1),
          Tdispatch(tag2, ctxs2, dst2, dtab2, req2)) =>
        assert(
          tag1 == tag2 && ctxs1 == ctxs2 && dst1 == dst2 &&
          Equiv[Dtab].equiv(dtab1, dtab2) && req1 == req2)
      case (a, b) => assert(a == b)
    }

    // Debugging tip: in an error message, 'm' is the RHS.
    for (m <- ms)
      assertEquiv(decode(encode(m)), m)
  }

  test("not encode invalid messages") {
    assert(intercept[BadMessageException] {
      encode(Treq(-1, Some(tracing.Trace.nextId), body))
    } == BadMessageException("invalid tag number -1"))
    assert(intercept[BadMessageException] {
      encode(Treq(1 << 24, Some(tracing.Trace.nextId), body))
    } == BadMessageException("invalid tag number 16777216"))
  }

  test("not decode invalid messages") {
    assert(intercept[BadMessageException] {
      decode(ChannelBuffers.EMPTY_BUFFER)
    } == BadMessageException("short message"))
    assert(intercept[BadMessageException] {
      decode(ChannelBuffers.wrappedBuffer(Array[Byte](0, 0, 0, 1)))
    } == BadMessageException("bad message type: 0 [tag=1]"))
  }

  test("extract control messages") {
    val tag = 0
    val buf = ChannelBuffers.EMPTY_BUFFER

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
}
