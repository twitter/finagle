package com.twitter.finagle.tracing

import com.twitter.conversions.time._
import com.twitter.util.Time
import org.junit.runner.RunWith
import org.scalatest.{OneInstancePerTest, BeforeAndAfter, FunSuite}
import org.scalatest.junit.JUnitRunner
import org.mockito.Mockito.{never, times, verify, when, atLeast}
import org.mockito.Matchers.any
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class TraceTest extends FunSuite with MockitoSugar with BeforeAndAfter with OneInstancePerTest {
  before { Trace.clear() }
  after { Trace.clear() }

  val Seq(id0, id1, id2) = 0 until 3 map { i =>
    TraceId(Some(SpanId(i)), Some(SpanId(i)), SpanId(i), None, Flags(i))
  }

  test("have a default id without parents, etc.") {
    assert(Trace.id match {
      case TraceId(None, None, _, None, Flags(0)) => true
      case _ => false
    })
  }

  test("Trace.setId: set a fresh id when none exist") {
    assert(Trace.idOption === None)

    val defaultId = Trace.id
    Trace.setId(Trace.nextId)
    assert(Trace.id != defaultId)
    assert(Trace.id match {
      case TraceId(None, None, _, None, Flags(0)) => true
      case _ => false
    })
  }


  test("Trace.setId: set a derived id when one exists") {
    Trace.setId(Trace.nextId)
    val topId = Trace.id
    Trace.setId(Trace.nextId)
    assert(Trace.id match {
      case TraceId(Some(traceId), Some(parentId), _, None, Flags(0))
        if (traceId == topId.traceId && parentId == topId.spanId) => true
      case _ => false
    })
  }

  test("Trace.setId: not set additional terminal id") {
    Trace.setId(Trace.nextId, true)
    val topId = Trace.id
    Trace.setId(Trace.nextId, true)
    assert(Trace.id === topId)
  }

  test("Trace.setId: not set id when terminal id exists") {
    Trace.setId(Trace.nextId, true)
    val topId = Trace.id
    Trace.setId(Trace.nextId)
    assert(Trace.id === topId)
  }

  val tracer1 = mock[Tracer]
  val tracer2 = mock[Tracer]

  test("Trace.traceService") {
    var didRun = false

    Trace.pushTracer(tracer1)
    val priorId = Trace.id

    Trace.traceService("service", "rpcname") {
      assert(Trace.id != priorId)
      didRun = true
    }

    verify(tracer1, atLeast(3)).record(any[Record])
    assert(didRun)
    assert(Trace.id === priorId)
  }

  test("Trace.unwind") {
    var didRun = false
    val priorId = Trace.id
    Trace.unwind {
      Trace.setId(id0)
      assert(Trace.id === id0)
      didRun = true
    }
    assert(didRun)
    assert(Trace.id === priorId)
  }

  test("Trace.record: report topmost id to all tracers") { Time.withCurrentTimeFrozen { tc =>
    Trace.setId(id0)
    Trace.pushTracer(tracer1)
    val ann = Annotation.Message("hello")
    Trace.record(ann)
    verify(tracer1, times(1)).record(any[Record])
    Trace.setId(id1)
    Trace.record(ann)
    verify(tracer1, times(1)).record(Record(id1, Time.now, ann))
    tc.advance(1.second)
    Trace.setId(id2)
    Trace.record(ann)
    verify(tracer1, times(1)).record(Record(id2, Time.now, ann))
    tc.advance(1.second)
    Trace.pushTracer(tracer2)
    Trace.setId(id0)
    Trace.record(ann)
    verify(tracer1, times(1)).record(Record(id0, Time.now, ann))
    verify(tracer2, times(1)).record(Record(id0, Time.now, ann))
  }}

  test("Trace.record: record IDs not in the stack to all tracers") {
    Time.withCurrentTimeFrozen { tc =>
      Trace.pushTracer(tracer1)
      Trace.setId(id0)
      Trace.pushTracer(tracer2)
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

  test("Trace.record: record binary annotations") { Time.withCurrentTimeFrozen { tc  =>
    Trace.pushTracer(tracer1)
    Trace.setId(id0)
    val rec1 = Record(id0, Time.now,
      Annotation.BinaryAnnotation("key", "test"))
    Trace.recordBinary("key", "test")
    verify(tracer1, times(1)).record(rec1)
  }}

  test("Trace.record: not report when tracing turned off") {
    try {
      Trace.disable
      Trace.pushTracer(tracer1)
      Trace.pushTracer(tracer2)
      Trace.setId(id0)
      verify(tracer1, never()).record(any[Record])
      verify(tracer2, never()).record(any[Record])
      Trace.record("oh hey")
      verify(tracer1, never()).record(any[Record])
      verify(tracer2, never()).record(any[Record])
    } finally {
      Trace.enable
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
    Trace.setId(id)
    val nextId = Trace.nextId
    assert(id.flags === nextId.flags)
  }

  test("set empty flags in next id if no current id set") {
    val nextId = Trace.nextId
    assert(nextId.flags === Flags())
  }

  test("Trace.traceWith: start with a default TraceId") {
    Time.withCurrentTimeFrozen { tc  =>
      val tracer = mock[Tracer]
      when(tracer.sampleTrace(any[TraceId])).thenReturn(None)

      Trace.pushTracerAndSetNextId(tracer)
      val currentId = Trace.id
      assert(currentId match {
        case TraceId(None, None, _, None, Flags(0)) => true
        case _ => false
      })
      assert(Trace.isTerminal === false)
      assert(Trace.tracers === List(tracer))
      Trace.record("Hello world")
      verify(tracer, times(1)).sampleTrace(currentId)
      verify(tracer, times(1)).record(Record(currentId, Time.now, Annotation.Message("Hello world"), None))
    }
  }

  test("Trace.traceWith: use parent's sampled if it is defined") {
    Time.withCurrentTimeFrozen { tc  =>
      val tracer = mock[Tracer]
      when(tracer.sampleTrace(any[TraceId])).thenReturn(Some(true))

      val parentId = TraceId(Some(SpanId(123)), Some(SpanId(456)), SpanId(789), Some(false), Flags(0))
      Trace.setId(parentId)
      Trace.pushTracerAndSetNextId(tracer)
      val currentId = Trace.id
      assert(currentId match {
        case TraceId(Some(_traceId), Some(_parentId), _, Some(_sampled), Flags(0))
          if (_traceId == parentId.traceId) && (_parentId == parentId.spanId) &&
            (_sampled == parentId.sampled.get) => true
        case _ => false
      })
      assert(Trace.isTerminal === false)
      assert(Trace.tracers === List(tracer))
      verify(tracer, never()).sampleTrace(currentId)
      Trace.record("Hello world")
      verify(tracer, never()).record(any[Record])
    }
  }

  test("Trace.traceWith: call with terminal=true") {
    Time.withCurrentTimeFrozen { tc  =>
      val tracer = mock[Tracer]
      when(tracer.sampleTrace(any[TraceId])).thenReturn(None)

      Trace.pushTracerAndSetNextId(tracer, true)
      val currentId = Trace.id
      assert(currentId match {
        case TraceId(None, None, _, None, Flags(0)) => true
        case _ => false
      })
      assert(Trace.isTerminal === true)
      assert(Trace.tracers === List(tracer))
      verify(tracer, times(1)).sampleTrace(currentId)
      Trace.record("Hello world")
      verify(tracer, times(1)).record(Record(currentId, Time.now, Annotation.Message("Hello world"), None))
    }
  }

  test("Trace.traceWith: trace with terminal set for the current state") {
    Time.withCurrentTimeFrozen { tc  =>
      val tracer = mock[Tracer]
      when(tracer.sampleTrace(any[TraceId])).thenReturn(Some(true))

      val parentId = TraceId(Some(SpanId(123)), Some(SpanId(456)), SpanId(789), Some(true), Flags(0))
      Trace.setId(parentId, terminal = true)
      Trace.pushTracerAndSetNextId(tracer)
      val currentId = Trace.id
      assert(currentId === parentId)
      assert(Trace.isTerminal === true)
      assert(Trace.tracers === List(tracer))
      verify(tracer, never()).sampleTrace(currentId)
      Trace.record("Hello world")
      verify(tracer, times(1)).record(Record(currentId, Time.now, Annotation.Message("Hello world"), None))
    }
  }

  test("Trace.isActivelyTracing") {
    val id = TraceId(Some(SpanId(12)), Some(SpanId(13)), SpanId(14), None, Flags(0L))
    val tracer = mock[Tracer]
    Trace.clear()
    assert(Trace.isActivelyTracing == false) // no tracers, not tracing
    Trace.setId(id)
    Trace.pushTracer(NullTracer)
    assert(Trace.isActivelyTracing == false) // only the null tracer, still false
    Trace.clear()
    Trace.setId(id)
    when(tracer.sampleTrace(any[TraceId])).thenReturn(None)
    Trace.pushTracer(tracer)
    assert(Trace.isActivelyTracing == true) // tracer/id is None/None, default to trace
    Trace.setId(id.copy(_sampled = Some(false)))
    assert(Trace.isActivelyTracing == false) // tracer/id is None/false, don't trace
    when(tracer.sampleTrace(any[TraceId])).thenReturn(Some(false))
    assert(Trace.isActivelyTracing == false) // false/false, better not
    Trace.setId(id.copy(_sampled = Some(false), flags = Flags().setDebug))
    assert(Trace.isActivelyTracing == true) // debug should force its way through
    when(tracer.sampleTrace(any[TraceId])).thenReturn(Some(true))
    Trace.setId(id.copy(_sampled=Some(false)))
    assert(Trace.isActivelyTracing == false) // true/false, prefer the trace id's opinion
    Trace.setId(id.copy(_sampled = Some(true)))
    assert(Trace.isActivelyTracing == true) // true/true better be true
    Trace.disable()
    assert(Trace.isActivelyTracing == false) // disabled with true/true should be false
    Trace.enable()
    when(tracer.sampleTrace(any[TraceId])).thenReturn(Some(false))
    assert(Trace.isActivelyTracing == true) // false/true again prefer the id's opinion
  }
}
