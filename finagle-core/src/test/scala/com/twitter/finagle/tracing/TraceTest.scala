package com.twitter.finagle.tracing

import com.twitter.conversions.time._
import com.twitter.io.Buf
import com.twitter.util.Time
import com.twitter.util.{Return, Throw}
import org.junit.runner.RunWith
import org.mockito.Matchers.any
import org.mockito.Mockito.{never, times, verify, when, atLeast}
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{OneInstancePerTest, BeforeAndAfter, FunSuite}
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TraceTest extends FunSuite with MockitoSugar with BeforeAndAfter with OneInstancePerTest {
  val Seq(id0, id1, id2) = 0 until 3 map { i =>
    TraceId(Some(SpanId(i)), Some(SpanId(i)), SpanId(i), None, Flags(i))
  }

  test("have a default id without parents, etc.") {
    assert(Trace.id match {
      case TraceId(None, None, _, None, Flags(0), None) => true
      case _ => false
    })
  }

  test("Trace.letTracer") {
    var runs = 0
    val tracer = mock[Tracer]

    assert(Trace.tracers.isEmpty)
    Trace.letTracer(tracer) {
      assert(Trace.tracers == List(tracer))
      runs += 1
    }

    assert(runs == 1)
  }

  test("Trace.letId") {
    var didRun = false
    val priorId = Trace.id
    Trace.letId(id0) {
      assert(Trace.id == id0)
      didRun = true
    }
    assert(didRun)
    assert(Trace.id == priorId)
  }

  test("Trace.letId: set a fresh id when none exist") {
    assert(Trace.idOption == None)

    val defaultId = Trace.id
    Trace.letId(Trace.nextId) {
      assert(Trace.id != defaultId)
      assert(Trace.id match {
        case TraceId(None, None, _, None, Flags(0), None) => true
        case _ => false
      })
    }
  }

  test("Trace.letId: set a derived id when one exists") {
    Trace.letId(Trace.nextId) {
      val topId = Trace.id
      Trace.letId(Trace.nextId) {
        assert(Trace.id match {
          case TraceId(Some(traceId), Some(parentId), _, None, Flags(0), _)
              if traceId == topId.traceId && parentId == topId.spanId =>
            true
          case _ => false
        })
      }
    }
  }

  test("Trace.letId: not set additional terminal id") {
    Trace.letId(Trace.nextId, true) {
      val topId = Trace.id
      Trace.letId(Trace.nextId, true) {
        assert(Trace.id == topId)
      }
    }
  }

  test("Trace.letId: not set id when terminal id exists") {
    Trace.letId(Trace.nextId, true) {
      val topId = Trace.id
      Trace.letId(Trace.nextId) {
        assert(Trace.id == topId)
      }
    }
  }

  val tracer1 = mock[Tracer]
  val tracer2 = mock[Tracer]

  when(tracer1.isActivelyTracing(any[TraceId])).thenReturn(true)
  when(tracer2.isActivelyTracing(any[TraceId])).thenReturn(true)

  test("Trace.traceService") {
    var didRun = false

    Trace.letTracer(tracer1) {
      val priorId = Trace.id

      Trace.traceService("service", "rpcname") {
        assert(Trace.id != priorId)
        didRun = true
      }

      verify(tracer1, atLeast(3)).record(any[Record])
      assert(Trace.id == priorId)
    }
    assert(didRun)
  }

  test("Trace.record: report topmost id to all tracers") {
    Time.withCurrentTimeFrozen { tc =>
      Trace.letTracerAndId(tracer1, id0) {
        val ann = Annotation.Message("hello")
        Trace.record(ann)
        verify(tracer1, times(1)).record(any[Record])
        Trace.letId(id1) {
          Trace.record(ann)
          verify(tracer1, times(1)).record(Record(id1, Time.now, ann))
          tc.advance(1.second)
          Trace.letId(id2) {
            Trace.record(ann)
            verify(tracer1, times(1)).record(Record(id2, Time.now, ann))
            tc.advance(1.second)
            Trace.letTracerAndId(tracer2, id0) {
              Trace.record(ann)
              verify(tracer1, times(1)).record(Record(id0, Time.now, ann))
              verify(tracer2, times(1)).record(Record(id0, Time.now, ann))
            }
          }
        }
      }
    }
  }

  test("Trace.record: record IDs not in the stack to all tracers") {
    Time.withCurrentTimeFrozen { tc =>
      Trace.letTracerAndId(tracer1, id0) {
        Trace.letTracer(tracer2) {
          val rec1 = Record(id1, Time.now, Annotation.Message("wtf"))
          Trace.record(rec1)
          verify(tracer1, times(1)).record(rec1)
          verify(tracer2, times(1)).record(rec1)
          val rec0 = Record(id0, Time.now, Annotation.Message("wtf0"))
          Trace.record(rec0)
          verify(tracer1, times(1)).record(rec0)
          verify(tracer2, times(1)).record(rec0)
        }
      }
    }
  }

  test("Trace.record: record binary annotations") {
    Time.withCurrentTimeFrozen { tc =>
      Trace.letTracerAndId(tracer1, id0) {
        val rec1 = Record(id0, Time.now, Annotation.BinaryAnnotation("key", "test"))
        Trace.recordBinary("key", "test")
        verify(tracer1, times(1)).record(rec1)
      }
    }
  }

  test("Trace.record: not report when tracing turned off") {
    try {
      Trace.disable()
      Trace.letTracer(tracer2) {
        Trace.letTracerAndId(tracer1, id0) {
          Trace.letTracer(tracer1) {
            Trace.letTracer(tracer2) {
              Trace.letId(id0) {
                verify(tracer1, never()).record(any[Record])
                verify(tracer2, never()).record(any[Record])
                Trace.record("oh hey")
                verify(tracer1, never()).record(any[Record])
                verify(tracer2, never()).record(any[Record])
              }
            }
          }
        }
      }
    } finally {
      Trace.enable()
    }
  }

  /* TODO temporarily disabled until we can mock stopwatches
      "Trace.time" in Time.withCurrentTimeFrozen { tc =>
        val tracer = new BufferingTracer()
        val duration = 1.second
        Trace.pushTracer(tracer)
        Trace.time("msg") {
          tc.advance(duration)
        }
        tracer.iterator foreach { r =>
          r.annotation mustEqual Annotation.Message("msg")
          r.duration mustEqual Some(duration)
        }
      }
   */

  test("pass flags to next id") {
    val flags = Flags().setDebug
    val id = TraceId(Some(SpanId(1L)), Some(SpanId(2L)), SpanId(3L), None, flags)
    Trace.letId(id) {
      val nextId = Trace.nextId
      assert(id.flags == nextId.flags)
    }
  }

  test("set empty flags in next id if no current id set") {
    val nextId = Trace.nextId
    assert(nextId.flags == Flags())
  }

  test("generates 64-bit SpanIDs by default") {
    val nextId = Trace.nextId
    assert(nextId.spanId.toString.length == 16)
  }

  test("Trace.letTracerAndNextId: start with a default TraceId") {
    Time.withCurrentTimeFrozen { tc =>
      val tracer = mock[Tracer]
      when(tracer.sampleTrace(any[TraceId])).thenReturn(None)
      when(tracer.isActivelyTracing(any[TraceId])).thenReturn(true)

      Trace.letTracerAndNextId(tracer) {
        val currentId = Trace.id
        assert(currentId match {
          case TraceId(None, None, _, None, Flags(0), None) => true
          case _ => false
        })
        assert(Trace.isTerminal == false)
        assert(Trace.tracers == List(tracer))
        Trace.record("Hello world")
        verify(tracer, times(1)).sampleTrace(currentId)
        verify(tracer, times(1))
          .record(Record(currentId, Time.now, Annotation.Message("Hello world"), None))
      }
    }
  }

  test("Trace.letTracerAndNextId: use parent's sampled if it is defined") {
    Time.withCurrentTimeFrozen { tc =>
      val tracer = mock[Tracer]
      when(tracer.sampleTrace(any[TraceId])).thenReturn(Some(true))

      val parentId =
        TraceId(Some(SpanId(123)), Some(SpanId(456)), SpanId(789), Some(false), Flags(0))
      Trace.letId(parentId) {
        Trace.letTracerAndNextId(tracer) {
          val currentId = Trace.id
          assert(currentId match {
            case TraceId(Some(_traceId), Some(_parentId), _, Some(_sampled), Flags(0), _)
                if (_traceId == parentId.traceId) && (_parentId == parentId.spanId) &&
                  (_sampled == parentId.sampled.get) =>
              true
            case _ => false
          })

          when(tracer.isActivelyTracing(currentId)).thenReturn(currentId.sampled.getOrElse(true))
          assert(Trace.isTerminal == false)
          assert(Trace.tracers == List(tracer))
          verify(tracer, never()).sampleTrace(currentId)
          Trace.record("Hello world")
          verify(tracer, never()).record(any[Record])
        }
      }
    }
  }

  test("Trace.letTracerAndNextId: call with terminal=true") {
    Time.withCurrentTimeFrozen { tc =>
      val tracer = mock[Tracer]
      when(tracer.sampleTrace(any[TraceId])).thenReturn(None)
      when(tracer.isActivelyTracing(any[TraceId])).thenReturn(true)

      Trace.letTracerAndNextId(tracer, true) {
        val currentId = Trace.id
        assert(currentId match {
          case TraceId(None, None, _, None, Flags(0), None) => true
          case _ => false
        })
        assert(Trace.isTerminal == true)
        assert(Trace.tracers == List(tracer))
        verify(tracer, times(1)).sampleTrace(currentId)
        Trace.record("Hello world")
        verify(tracer, times(1))
          .record(Record(currentId, Time.now, Annotation.Message("Hello world"), None))
      }
    }
  }

  test("Trace.letTracerAndNextId: trace with terminal set for the current state") {
    Time.withCurrentTimeFrozen { tc =>
      val tracer = mock[Tracer]
      when(tracer.sampleTrace(any[TraceId])).thenReturn(Some(true))
      when(tracer.isActivelyTracing(any[TraceId])).thenReturn(true)

      val parentId =
        TraceId(Some(SpanId(123)), Some(SpanId(456)), SpanId(789), Some(true), Flags(0))
      Trace.letId(parentId, terminal = true) {
        Trace.letTracerAndNextId(tracer) {
          val currentId = Trace.id
          assert(currentId == parentId)
          assert(Trace.isTerminal == true)
          assert(Trace.tracers == List(tracer))
          verify(tracer, never()).sampleTrace(currentId)
          Trace.record("Hello world")
          verify(tracer, times(1))
            .record(Record(currentId, Time.now, Annotation.Message("Hello world"), None))
        }
      }
    }
  }

  test("Trace.isActivelyTracing") {
    val id = TraceId(Some(SpanId(12)), Some(SpanId(13)), SpanId(14), None, Flags(0L))
    val tracer1 = mock[Tracer]
    val tracer2 = mock[Tracer]
    val tracer = BroadcastTracer(Seq(tracer1, tracer2))

    // no tracers, not tracing
    assert(!Trace.isActivelyTracing)

    // only the null tracer, still false
    Trace.letTracerAndId(NullTracer, id) {
      assert(!Trace.isActivelyTracing)
    }

    Trace.letTracer(tracer) {
      Trace.letId(id) {
        // Even if one tracer returns, then true
        when(tracer1.isActivelyTracing(any[TraceId])).thenReturn(false)
        when(tracer2.isActivelyTracing(any[TraceId])).thenReturn(true)
        assert(Trace.isActivelyTracing)

        // when everything returns true, then true
        when(tracer1.isActivelyTracing(any[TraceId])).thenReturn(true)
        when(tracer2.isActivelyTracing(any[TraceId])).thenReturn(true)
        assert(Trace.isActivelyTracing)

        // tracing enabled flag overrides individual tracer decisions
        when(tracer1.isActivelyTracing(any[TraceId])).thenReturn(true)
        when(tracer2.isActivelyTracing(any[TraceId])).thenReturn(true)
        Trace.disable()
        assert(!Trace.isActivelyTracing)
        Trace.enable()
        assert(Trace.isActivelyTracing)

        // when everything returns false, then false
        when(tracer1.isActivelyTracing(any[TraceId])).thenReturn(false)
        when(tracer2.isActivelyTracing(any[TraceId])).thenReturn(false)
        assert(!Trace.isActivelyTracing)
      }
    }
  }

  test("trace ID serialization: valid ids (64-bit)") {
    // TODO: Consider using scalacheck here. (CSL-595)
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

    for (id <- traceIds)
      assert(Trace.idCtx.tryUnmarshal(Trace.idCtx.marshal(id)) == Return(id))
  }

  test("trace ID serialization: valid ids (128-bit)") {
    val traceId = TraceId(Some(SpanId(1L)), Some(SpanId(1L)), SpanId(2L), None, Flags(Flags.Debug), Some(SpanId(2L)))

    assert(Trace.idCtx.tryUnmarshal(Trace.idCtx.marshal(traceId)) == Return(traceId))
  }

  // example from X-Amzn-Trace-Id: Root=1-5759e988-bd862e3fe1be46a994272793;Sampled=1
  test("Trace.nextTraceIdHigh: encodes epoch seconds") {
    Time.withTimeAt(Time.fromSeconds(1465510280)) { tc => // Thursday, June 9, 2016 10:11:20 PM
      val traceIdHigh = Trace.nextTraceIdHigh()
      assert(traceIdHigh.toString.startsWith("5759e988")) == true
    }
  }

  test("trace ID serialization: throw in handle on invalid size") {
    val bytes = new Array[Byte](33)

    Trace.idCtx.tryUnmarshal(Buf.ByteArray.Owned(bytes)) match {
      case Throw(_: IllegalArgumentException) =>
      case rv => fail(s"Got $rv")
    }
  }
}
