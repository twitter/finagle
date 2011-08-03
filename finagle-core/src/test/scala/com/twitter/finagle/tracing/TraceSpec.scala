package com.twitter.finagle.tracing

import org.specs.Specification
import org.specs.mock.Mockito

import com.twitter.util.Time
import com.twitter.conversions.time._

object TraceSpec extends Specification with Mockito {
  "Trace" should {
    doBefore { Trace.clear() }

    val Seq(id0, id1, id2) = 0 until 3 map { i =>
      TraceId(Some(SpanId(i)), Some(SpanId(i)), SpanId(i), None)
    }

    "have a default id without parents, etc." in {
      Trace.id must beLike {
        case TraceId(None, None, _, None) => true
      }
    }

    "keep ids in a stack" in {
      Trace.pushId(id0)
      Trace.id must be_==(id0)
      Trace.pushId(id1)
      Trace.id must be_==(id1)
      Trace.pushId(id2)
      Trace.id must be_==(id2)

      Trace.popId()
      Trace.id must be_==(id1)
      Trace.popId()
      Trace.id must be_==(id0)
      Trace.popId()
      Trace.id must beLike {  // back to default
        case TraceId(None, None, _, None) => true
      }
    }

    "Trace.pushId" in {
      "push a fresh id when none exist" in {
        Trace.idOption must beNone
        val defaultId = Trace.id
        Trace.pushId()
        Trace.id must be_!=(defaultId)
        Trace.id must beLike {
          case TraceId(None, None, _, None) => true
        }
      }

      "push a derived id when one exists" in {
        Trace.pushId()
        val topId = Trace.id
        Trace.pushId()
        Trace.id must beLike {
          case TraceId(Some(traceId), Some(parentId), _, None)
          if (traceId == topId.traceId && parentId == topId.spanId) => true
        }
      }
    }

    "Trace.unwind" in {
      var didRun = false
      val priorId = Trace.id
      Trace.unwind {
        Trace.pushId(id0)
        Trace.id must be_==(id0)
        didRun = true
      }
      didRun must beTrue
      Trace.id must be_==(priorId)
    }

    "Trace.record" in {
      val tracer1 = mock[Tracer]
      val tracer2 = mock[Tracer]

      "report topmost id to tracers below this id" in Time.withCurrentTimeFrozen { tc =>
        Trace.pushId(id0)
        Trace.pushTracer(tracer1)
        val ann = Annotation.Message("hello")
        Trace.record(ann)
        there was no(tracer1).record(any)
        Trace.pushId(id1)
        Trace.record(ann)
        there was one(tracer1).record(Record(id1, Time.now, ann))
        tc.advance(1.second)
        Trace.pushId(id2)
        Trace.record(ann)
        there was one(tracer1).record(Record(id2, Time.now, ann))
        tc.advance(1.second)
        Trace.pushTracer(tracer2)
        Trace.pushId(id0)
        Trace.record(ann)
        there was one(tracer1).record(Record(id0, Time.now, ann))
        there was one(tracer2).record(Record(id0, Time.now, ann))
      }

      "record IDs not in the stack to all tracers" in {
        Trace.pushTracer(tracer1)
        Trace.pushId(id0)
        Trace.pushTracer(tracer2)
        val rec1 = Record(id1, Time.now, Annotation.Message("wtf"))
        Trace.record(rec1)
        there was one(tracer1).record(rec1)
        there was one(tracer2).record(rec1)
        val rec0 = Record(id0, Time.now, Annotation.Message("wtf0"))
        Trace.record(rec0)
        there was one(tracer1).record(rec0)
        there was no(tracer2).record(rec0)
      }

      "report to each unique tracer exactly once" in {
        Trace.pushTracer(tracer1)
        Trace.pushTracer(tracer2)
        Trace.pushTracer(tracer1)
        Trace.pushId(id0)
        there was no(tracer1).record(any)
        there was no(tracer2).record(any)
        Trace.record("oh hey")
        there was one(tracer1).record(any)
        there was one(tracer2).record(any)
      }
    }
  }
}
