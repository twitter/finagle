package com.twitter.finagle.tracing

import com.twitter.conversions.time._
import com.twitter.util.Time

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

class TraceSpec extends SpecificationWithJUnit with Mockito {
  "Trace" should {
    doBefore { Trace.clear() }

    val Seq(id0, id1, id2) = 0 until 3 map { i =>
      TraceId(Some(SpanId(i)), Some(SpanId(i)), SpanId(i), None, Flags(i))
    }

    "have a default id without parents, etc." in {
      Trace.id must beLike {
        case TraceId(None, None, _, None, Flags(0)) => true
      }
    }

    "Trace.set" in {
      "set a fresh id when none exist" in {
        Trace.idOption must beNone
        val defaultId = Trace.id
        Trace.setId(Trace.nextId)
        Trace.id must be_!=(defaultId)
        Trace.id must beLike {
          case TraceId(None, None, _, None, Flags(0)) => true
        }
      }

      "set a derived id when one exists" in {
        Trace.setId(Trace.nextId)
        val topId = Trace.id
        Trace.setId(Trace.nextId)
        Trace.id must beLike {
          case TraceId(Some(traceId), Some(parentId), _, None, Flags(0))
          if (traceId == topId.traceId && parentId == topId.spanId) => true
        }
      }

      "not set additional terminal id" in {
        Trace.setId(Trace.nextId, true)
        val topId = Trace.id
        Trace.setId(Trace.nextId, true)
        Trace.id must be_==(topId)
      }

      "not set id when terminal id exists" in {
        Trace.setId(Trace.nextId, true)
        val topId = Trace.id
        Trace.setId(Trace.nextId)
        Trace.id must be_==(topId)
      }
    }

    "Trace.unwind" in {
      var didRun = false
      val priorId = Trace.id
      Trace.unwind {
        Trace.setId(id0)
        Trace.id must be_==(id0)
        didRun = true
      }
      didRun must beTrue
      Trace.id must be_==(priorId)
    }

    "Trace.record" in {
      val tracer1 = mock[Tracer]
      val tracer2 = mock[Tracer]

      "report topmost id to all tracers" in Time.withCurrentTimeFrozen { tc =>
        Trace.setId(id0)
        Trace.pushTracer(tracer1)
        val ann = Annotation.Message("hello")
        Trace.record(ann)
        there was one(tracer1).record(any)
        Trace.setId(id1)
        Trace.record(ann)
        there was one(tracer1).record(Record(id1, Time.now, ann))
        tc.advance(1.second)
        Trace.setId(id2)
        Trace.record(ann)
        there was one(tracer1).record(Record(id2, Time.now, ann))
        tc.advance(1.second)
        Trace.pushTracer(tracer2)
        Trace.setId(id0)
        Trace.record(ann)
        there was one(tracer1).record(Record(id0, Time.now, ann))
        there was one(tracer2).record(Record(id0, Time.now, ann))
      }

      "record IDs not in the stack to all tracers" in {
        Trace.pushTracer(tracer1)
        Trace.setId(id0)
        Trace.pushTracer(tracer2)
        val rec1 = Record(id1, Time.now, Annotation.Message("wtf"))
        Trace.record(rec1)
        there was one(tracer1).record(rec1)
        there was one(tracer2).record(rec1)
        val rec0 = Record(id0, Time.now, Annotation.Message("wtf0"))
        Trace.record(rec0)
        there was one(tracer1).record(rec0)
        there was one(tracer2).record(rec0)
      }

      "record binary annotations" in Time.withCurrentTimeFrozen { tc  =>
        Trace.pushTracer(tracer1)
        Trace.setId(id0)
        val rec1 = Record(id0, Time.now,
          Annotation.BinaryAnnotation("key", "test"))
        Trace.recordBinary("key", "test")
        there was one(tracer1).record(rec1)
      }

      "not report when tracing turned off" in {
        try {
          Trace.disable
          Trace.pushTracer(tracer1)
          Trace.pushTracer(tracer2)
          Trace.setId(id0)
          there was no(tracer1).record(any)
          there was no(tracer2).record(any)
          Trace.record("oh hey")
          there was no(tracer1).record(any)
          there was no(tracer2).record(any)
        } finally {
          Trace.enable
        }
      }
    }

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

    "pass flags to next id" in {
      val flags = Flags().setDebug
      val id = TraceId(Some(SpanId(1L)), Some(SpanId(2L)), SpanId(3L), None, flags)
      Trace.setId(id)
      val nextId = Trace.nextId
      id.flags mustEqual nextId.flags
    }

    "set empty flags in next id if no current id set" in {
      val nextId = Trace.nextId
      nextId.flags mustEqual Flags()
    }

  }
}
