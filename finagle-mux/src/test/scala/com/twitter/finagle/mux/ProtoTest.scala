package com.twitter.finagle.mux

import com.twitter.finagle.tracing
import org.jboss.netty.buffer.ChannelBuffers
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import scala.collection.mutable
import org.jboss.netty.util.CharsetUtil

@RunWith(classOf[JUnitRunner])
class ProtoTest extends FunSuite {
  def buf(n: Int) = ChannelBuffers.wrappedBuffer((0 until n).toArray.map(_.toByte))
  val body = buf(4)
  
  val goodTags = Seq(8388607, 1, 123)
  val goodTraceIds = Seq(None, Some(tracing.Trace.nextId))
  val goodBufs = Seq(ChannelBuffers.EMPTY_BUFFER, buf(1), buf(4), buf(100))
  val goodStrings = Seq("", "Hello, world!", "☺☹")
  val goodKeys = goodStrings map { s =>
    val bytes = s.getBytes(CharsetUtil.UTF_8)
    ChannelBuffers.wrappedBuffer(bytes)
  }
  val goodContexts = 
    Seq() ++ (for { k <- goodKeys; v <- goodBufs } yield (k, v)).combinations(2).toSeq

  import Message._

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
      body <- goodBufs
    } yield Tdispatch(tag, ctx, body))
    
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

    // Debugging tip: in an error message, 'm' is the RHS.
    for (m <- ms)
      assert(decode(encode(m)) === m)
  }

  test("not encode invalid messages") {
    assert(intercept[BadMessageException] {
      encode(Treq(-1, Some(tracing.Trace.nextId), body))
    } === BadMessageException("invalid tag number -1"))
    assert(intercept[BadMessageException] {
      encode(Treq(0, Some(tracing.Trace.nextId), body))
    } === BadMessageException("invalid tag number 0"))
    assert(intercept[BadMessageException] {
      encode(Treq(1<<24, Some(tracing.Trace.nextId), body))
    } === BadMessageException("invalid tag number 16777216"))
  }


  test("not decode invalid messages") {
    assert(intercept[BadMessageException] {
      decode(ChannelBuffers.EMPTY_BUFFER)
    } === BadMessageException("short message"))
    assert(intercept[BadMessageException] {
      decode(ChannelBuffers.wrappedBuffer(Array[Byte](0, 0, 0, 1)))
    } === BadMessageException("bad message type: 0 [tag=1]"))
  }
}
