package com.twitter.finagle.tracing

import com.twitter.io.Buf
import org.junit.runner.RunWith
import org.scalatest.{OneInstancePerTest, BeforeAndAfter, FunSuite}
import org.scalatest.junit.JUnitRunner
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TraceContextTest
  extends FunSuite
  with BeforeAndAfter with OneInstancePerTest {

  before { Trace.clear() }
  after { Trace.clear() }

  def longs(seed: Long) = {
    val rng = new Random(seed)
    Seq.fill(10) { rng.nextLong() }
  }

  def spanIds(seed: Long): Seq[Option[SpanId]] =
    None +: (longs(seed) map (l => Some(SpanId(l))))

  val traceIds = for {
    traceId <- spanIds(1L)
    parentId <- traceId +: spanIds(2L)
    maybeSpanId <- parentId +: spanIds(3L)
    spanId <- maybeSpanId.toSeq
    flags <- Seq(Flags(0L), Flags(Flags.Debug))
    sampled <- Seq(None, Some(false), Some(true))
  } yield TraceId(traceId, parentId, spanId, sampled, flags)

  val handler = new TraceContext

  test("Emit/handle") {
    for (id <- traceIds) {
      Trace.setId(id)
      val Some(buf) = handler.emit()
      Trace.clear()
      handler.handle(buf)

      assert(Trace.id === id)
      assert(Trace.id.sampled === id.sampled)
      assert(Trace.id.flags.isDebug === id.flags.isDebug)
    }
  }

  // TODO: Consider using scalacheck here. (CSL-595)
  test("throw in handle on invalid size") {
    val bytes = new Array[Byte](33)

    intercept[IllegalArgumentException] {
      handler.handle(Buf.ByteArray(bytes))
    }
  }
}
