package com.twitter.finagle.tracing

import com.twitter.conversions.time._
import com.twitter.util.Time
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{OneInstancePerTest, BeforeAndAfter, FunSuite}
import scala.language.reflectiveCalls

@RunWith(classOf[JUnitRunner])
class TraceTest extends FunSuite with BeforeAndAfter with OneInstancePerTest with MockitoSugar {
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
        if traceId == topId.traceId && parentId == topId.spanId => true
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

  test("Trace.state") {
    val state = Trace.state
    Trace.pushTracer(new BufferingTracer)
    Trace.state = state
    assert(Trace.state === state)
  }

  test("Trace.traceService") {
    val tracer1 = new BufferingTracer
    var didRun = false

    Trace.pushTracer(tracer1)
    val priorId = Trace.id

    Trace.traceService("service", "rpcname") {
      assert(Trace.id != priorId)
      didRun = true
    }

    assert(tracer1.size >= 3)
    assert(didRun)
    assert(Trace.id === priorId)
  }
  
  test("Trace.letTracerAndId") {
    val tracer = new BufferingTracer
    val id = Trace.nextId
    var runs = 0

    Trace.letTracerAndId(tracer, id) {
      runs += 1
      Trace.record("test message")
    }

    val Seq(Record(`id`, _, Annotation.Message("test message"), None)) = tracer.toSeq
    assert(runs === 1)
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

  test("Trace.record: report topmost id to all tracers") {
    val tracer1 = new BufferingTracer
    val tracer2 = new BufferingTracer

    Time.withCurrentTimeFrozen { tc =>
      Trace.setId(id0)
      Trace.pushTracer(tracer1)
      val ann = Annotation.Message("hello")
      Trace.record(ann)
      assert(tracer1.iterator.size === 1)
      Trace.setId(id1)
      Trace.record(ann)
      assert(tracer1.filter(_ == Record(id1, Time.now, ann)).size === 1)
      tc.advance(1.second)
      Trace.setId(id2)
      Trace.record(ann)
      assert(tracer1.filter(_ == Record(id2, Time.now, ann)).size === 1)
      tc.advance(1.second)
      Trace.pushTracer(tracer2)
      Trace.setId(id0)
      Trace.record(ann)
      assert(tracer1.filter(_ == Record(id0, Time.now, ann)).size === 1)
      assert(tracer2.filter(_ == Record(id0, Time.now, ann)).size === 1)
    }
  }

  test("Trace.record: record IDs not in the stack to all tracers") {
    val tracer1 = new BufferingTracer
    val tracer2 = new BufferingTracer

    Time.withCurrentTimeFrozen { tc =>
      Trace.pushTracer(tracer1)
      Trace.setId(id0)
      Trace.pushTracer(tracer2)
      val rec1 = Record(id1, Time.now, Annotation.Message("wtf"))
      Trace.record(rec1)
      assert(tracer1.filter(_ == rec1).size === 1)
      assert(tracer2.filter(_ == rec1).size === 1)
      val rec0 = Record(id0, Time.now, Annotation.Message("wtf0"))
      Trace.record(rec0)
      assert(tracer1.filter(_ == rec0).size === 1)
      assert(tracer2.filter(_ == rec0).size === 1)
    }
  }

  test("Trace.record: record binary annotations") {
    val tracer1 = new BufferingTracer

    Time.withCurrentTimeFrozen { tc =>
      Trace.pushTracer(tracer1)
      Trace.setId(id0)
      val rec1 = Record(id0, Time.now,
        Annotation.BinaryAnnotation("key", "test"))
      Trace.recordBinary("key", "test")
      assert(tracer1.filter(_ == rec1).size === 1)
    }
  }

  test("Trace.record: not report when tracing turned off") {
    val tracer1 = new BufferingTracer
    val tracer2 = new BufferingTracer

    try {
      Trace.disable
      Trace.pushTracer(tracer1)
      Trace.pushTracer(tracer2)
      Trace.setId(id0)
      assert(tracer1.size === 0)
      assert(tracer2.size === 0)
      Trace.record("oh hey")
      assert(tracer1.size === 0)
      assert(tracer2.size === 0)
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
    Time.withCurrentTimeFrozen { tc =>
      val tracer = new BufferingTracer {
        var checked = false
        override def sampleTrace(traceId: TraceId): Option[Boolean] = {
          checked = true
          None
        }
      }

      Trace.pushTracerAndSetNextId(tracer)
      val currentId = Trace.id
      assert(currentId match {
        case TraceId(None, None, _, None, Flags(0)) => true
        case _ => false
      })
      assert(Trace.isTerminal === false)
      assert(Trace.tracers === List(tracer))
      Trace.record("Hello world")
      assert(tracer.checked)
      assert(tracer.filter(_ == Record(currentId, Time.now, Annotation.Message("Hello world"), None)).size === 1)
    }
  }

  test("Trace.traceWith: use parent's sampled if it is defined") {
    Time.withCurrentTimeFrozen { tc =>
      val tracer = new BufferingTracer {
        var checked = false
        override def sampleTrace(traceId: TraceId): Option[Boolean] = {
          checked = true
          Some(true)
        }
      }

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
      assert(!tracer.checked)
      Trace.record("Hello world")
      assert(tracer.size === 0)
    }
  }

  test("Trace.traceWith: call with terminal=true") {
    Time.withCurrentTimeFrozen { tc =>
      val tracer = new BufferingTracer {
        var checked = false
        override def sampleTrace(traceId: TraceId): Option[Boolean] = {
          checked = true
          None
        }
      }

      Trace.pushTracerAndSetNextId(tracer, true)
      val currentId = Trace.id
      assert(currentId match {
        case TraceId(None, None, _, None, Flags(0)) => true
        case _ => false
      })
      assert(Trace.isTerminal === true)
      assert(Trace.tracers === List(tracer))
      assert(tracer.checked)
      Trace.record("Hello world")
      assert(tracer.filter(_ == Record(currentId, Time.now, Annotation.Message("Hello world"), None)).size === 1)
    }
  }

  test("Trace.traceWith: trace with terminal set for the current state") {
    Time.withCurrentTimeFrozen { tc =>
      val tracer = new BufferingTracer {
        var checked = false
        override def sampleTrace(traceId: TraceId): Option[Boolean] = {
          checked = true
          Some(true)
        }
      }

      val parentId = TraceId(Some(SpanId(123)), Some(SpanId(456)), SpanId(789), Some(true), Flags(0))
      Trace.setId(parentId, terminal = true)
      Trace.pushTracerAndSetNextId(tracer)
      val currentId = Trace.id
      assert(currentId === parentId)
      assert(Trace.isTerminal === true)
      assert(Trace.tracers === List(tracer))
      assert(!tracer.checked)
      Trace.record("Hello world")
      assert(tracer.filter(_ == Record(currentId, Time.now, Annotation.Message("Hello world"), None)).size === 1)
    }
  }

  test("Trace.isActivelyTracing") {
    val id = TraceId(Some(SpanId(12)), Some(SpanId(13)), SpanId(14), None, Flags(0L))
    val tracer = new BufferingTracer {
      var doSample: Option[Boolean] = None
      override def sampleTrace(traceId: TraceId): Option[Boolean] = doSample
    }

    Trace.clear()
    assert(!Trace.isActivelyTracing) // no tracers, not tracing
    Trace.setId(id)
    Trace.pushTracer(NullTracer)
    assert(!Trace.isActivelyTracing) // only the null tracer, still false
    Trace.clear()
    Trace.setId(id)
    Trace.pushTracer(tracer)
    assert(Trace.isActivelyTracing) // tracer/id is None/None, default to trace
    Trace.setId(id.copy(_sampled = Some(false)))
    assert(!Trace.isActivelyTracing) // tracer/id is None/false, don't trace
    tracer.doSample = Some(false)
    assert(!Trace.isActivelyTracing) // false/false, better not
    Trace.setId(id.copy(_sampled = Some(false), flags = Flags().setDebug))
    assert(Trace.isActivelyTracing) // debug should force its way through
    tracer.doSample = Some(true)
    Trace.setId(id.copy(_sampled = Some(false)))
    assert(!Trace.isActivelyTracing) // true/false, prefer the trace id's opinion
    Trace.setId(id.copy(_sampled = Some(true)))
    assert(Trace.isActivelyTracing) // true/true better be true
    Trace.disable()
    assert(!Trace.isActivelyTracing) // disabled with true/true should be false
    Trace.enable()
    tracer.doSample = Some(false)
    assert(Trace.isActivelyTracing) // false/true again prefer the id's opinion
    Trace.setId(id.copy(_sampled = None))
    tracer.doSample = Some(true)
    assert(Trace.isActivelyTracing) // tracer would like to trace this
  }

  test("Trace.isActivelyTracing: trace id with SamplingKnown flag set") {
    val id = TraceId(Some(SpanId(12)), Some(SpanId(13)), SpanId(14), Some(true), Flags(Flags.SamplingKnown | Flags.Sampled))
    val tracer = mock[Tracer]
    Trace.clear()
    Trace.pushTracer(tracer)
    Trace.setId(id)
    assert(Trace.isActivelyTracing === true)
    Trace.setId(id.copy(_sampled = Some(false), flags = Flags(Flags.SamplingKnown)))
    assert(Trace.isActivelyTracing === false)
  }
}
