package com.twitter.finagle.tracing

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.tracing.Annotation.BinaryAnnotation
import com.twitter.finagle.tracing.TraceTest.TraceIdException
import com.twitter.io.Buf
import com.twitter.util.Await
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.MockTimer
import com.twitter.util.Return
import com.twitter.util.Throw
import com.twitter.util.Time
import org.mockito.Matchers.any
import org.mockito.Mockito.atLeast
import org.mockito.Mockito.never
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfter
import org.scalatest.OneInstancePerTest
import org.scalatestplus.mockito.MockitoSugar
import scala.util.Random
import org.scalatest.funsuite.AnyFunSuite

class TraceTest extends AnyFunSuite with MockitoSugar with BeforeAndAfter with OneInstancePerTest {
  val Seq(id0, id1, id2) = 0 until 3 map { i =>
    TraceId(Some(SpanId(i)), Some(SpanId(i)), SpanId(i), None, Flags(i))
  }

  test("have a default id without parents, etc.") {
    assert(Trace.id match {
      case TraceId(None, None, _, None, Flags(0), None, _) => true
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

  test("Trace.letTracers: should enable all tracers in the stack") {
    var runs = 0
    val tracers = Seq(mock[Tracer], mock[Tracer])

    assert(Trace.tracers.isEmpty)
    Trace.letTracers(tracers) {
      assert(Trace.tracers == tracers)
      runs += 1
    }

    assert(runs == 1)
  }

  test("Trace.letTracers: should not replace existing tracers") {
    var runs = 0
    val tracer = mock[Tracer]
    val otherTracer = mock[Tracer]

    assert(Trace.tracers.isEmpty)
    Trace.letTracer(tracer) {
      assert(Trace.tracers == Seq(tracer))
      Trace.letTracers(Seq(otherTracer)) {
        assert(Trace.tracers == Seq(otherTracer, tracer))
        runs += 1
      }
      assert(Trace.tracers == Seq(tracer))
      runs += 1
    }

    assert(runs == 2)
  }

  test("Trace.letTracers: should not add any tracers when they already exist") {
    var runs = 0
    val tracer = mock[Tracer]
    val otherTracer = mock[Tracer]

    assert(Trace.tracers.isEmpty)
    Trace.letTracer(tracer) {
      assert(Trace.tracers == Seq(tracer))
      Trace.letTracer(otherTracer) {
        assert(Trace.tracers == Seq(otherTracer, tracer))
        Trace.letTracers(Seq(otherTracer, tracer)) {
          assert(Trace.tracers == Seq(otherTracer, tracer))
          runs += 1
        }
        assert(Trace.tracers == Seq(otherTracer, tracer))
        runs += 1
      }
      assert(Trace.tracers == Seq(tracer))
      runs += 1
    }

    assert(runs == 3)
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
        case TraceId(None, None, _, None, Flags(0), None, _) => true
        case _ => false
      })
    }
  }

  test("Trace.letId: set a derived id when one exists") {
    Trace.letId(Trace.nextId) {
      val topId = Trace.id
      Trace.letId(Trace.nextId) {
        assert(Trace.id match {
          case TraceId(Some(traceId), Some(parentId), _, None, Flags(0), _, _)
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
  when(tracer1.sampleTrace(any[TraceId])).thenReturn(None)
  when(tracer2.isActivelyTracing(any[TraceId])).thenReturn(true)
  when(tracer2.sampleTrace(any[TraceId])).thenReturn(None)

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

  test("Trace.record: record call site") {
    Time.withCurrentTimeFrozen { tc =>
      Trace.letTracerAndId(tracer1, id0) {
        val function = "foo$1"
        val namespace = "com.twitter.finagle.tracing.TraceTest"
        val filepath =
          getClass().getClassLoader().getResource(namespace.replace('.', '/') + ".class").toString
        val line = Thread.currentThread().getStackTrace()(1).getLineNumber()
        val lineno = line + 7
        val functionAnn = Annotation.BinaryAnnotation("code.function", function)
        val namespaceAnn = Annotation.BinaryAnnotation("code.namespace", namespace)
        val filePathAnn = Annotation.BinaryAnnotation("code.filepath", filepath)
        val lineNoAnn = Annotation.BinaryAnnotation("code.lineno", lineno)
        def foo(): Unit = {
          Trace.recordCallSite()
        }
        foo()
        verify(tracer1, times(1)).record(Record(id0, Time.now, functionAnn))
        verify(tracer1, times(1)).record(Record(id0, Time.now, namespaceAnn))
        verify(tracer1, times(1)).record(Record(id0, Time.now, filePathAnn))
        verify(tracer1, times(1)).record(Record(id0, Time.now, lineNoAnn))
      }
    }
  }

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
          case TraceId(None, None, _, None, Flags(0), None, _) => true
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
            case TraceId(Some(_traceId), Some(_parentId), _, Some(_sampled), Flags(0), _, _)
                if (_traceId == parentId.traceId) && (_parentId == parentId.spanId) &&
                  (_sampled == parentId.sampled.get) =>
              true
            case _ => false
          })

          when(tracer.isActivelyTracing(currentId)).thenReturn(currentId.sampled.getOrElse(true))
          assert(Trace.isTerminal == false)
          assert(Trace.tracers == List(tracer))
          verify(tracer, never()).sampleTrace(currentId)
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
          case TraceId(None, None, _, None, Flags(0), None, _) => true
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
      assert(Trace.TraceIdContext.tryUnmarshal(Trace.TraceIdContext.marshal(id)) == Return(id))
  }

  test("trace ID serialization: valid ids (128-bit)") {
    val traceId = TraceId(
      Some(SpanId(1L)),
      Some(SpanId(1L)),
      SpanId(2L),
      None,
      Flags(Flags.Debug),
      Some(SpanId(2L))
    )

    assert(
      Trace.TraceIdContext.tryUnmarshal(Trace.TraceIdContext.marshal(traceId)) == Return(traceId))
  }

  // example from X-Amzn-Trace-Id: Root=1-5759e988-bd862e3fe1be46a994272793;Sampled=1
  test("Trace.nextTraceIdHigh: encodes epoch seconds") {
    Time.withTimeAt(Time.fromSeconds(1465510280)) { tc => // Thursday, June 9, 2016 10:11:20 PM
      val traceIdHigh = Tracing.nextTraceIdHigh(new java.util.Random)
      assert(traceIdHigh.toString.startsWith("5759e988"))
    }
  }

  test("Trace.nextTraceIdHigh: high 32 bits change after 2038-01-19 03:14:07Z") {
    Time.withTimeAt(Time.fromSeconds(Int.MaxValue) + Duration.fromSeconds(2)) { tc =>
      val traceIdHigh = Tracing.nextTraceIdHigh(new java.util.Random)
      assert(traceIdHigh.toString.startsWith("80000001"))
    }
  }

  test("trace ID serialization: throw in handle on invalid size") {
    val bytes = new Array[Byte](33)

    Trace.TraceIdContext.tryUnmarshal(Buf.ByteArray.Owned(bytes)) match {
      case Throw(_: IllegalArgumentException) =>
      case rv => fail(s"Got $rv")
    }
  }

  test("trace local span") {
    val startTime = Time.now
    Time.withTimeAt(startTime) { ctrl =>
      val tracer = new BufferingTracer()
      val parentTraceId = Trace.id
      val name = "work"
      Trace.letTracerAndId(tracer, parentTraceId) {
        val childTraceId = Trace.traceLocal(name) {
          ctrl.advance(1.second)
          Trace.id
        }

        assert(tracer.toSeq.contains(Record(childTraceId, startTime, Annotation.Rpc(name))))
        assert(
          tracer.toSeq.contains(Record(childTraceId, startTime, Annotation.ServiceName("local"))))
        assert(
          tracer.toSeq.contains(
            Record(childTraceId, startTime, Annotation.BinaryAnnotation("lc", name))))
        assert(
          tracer.toSeq.contains(Record(childTraceId, startTime, Annotation.Message("local/begin"))))
        assert(
          tracer.toSeq.contains(
            Record(childTraceId, startTime.plus(1.second), Annotation.Message("local/end"))))
        assert(parentTraceId != childTraceId)
      }
    }
  }

  test("trace local exceptional span") {
    val startTime = Time.now
    Time.withTimeAt(startTime) { ctrl =>
      val tracer = new BufferingTracer()
      val parentTraceId = Trace.id
      val name = "work"
      Trace.letTracerAndId(tracer, parentTraceId) {

        try {
          Trace.traceLocal(name) {
            ctrl.advance(1.second)
            throw TraceIdException(Trace.id)
          }
          fail("Expected exception to be thrown")
        } catch {
          case TraceIdException(childTraceId) =>
            assert(tracer.toSeq.contains(Record(childTraceId, startTime, Annotation.Rpc(name))))
            assert(
              tracer.toSeq.contains(
                Record(childTraceId, startTime, Annotation.ServiceName("local"))))
            assert(
              tracer.toSeq.contains(
                Record(childTraceId, startTime, Annotation.BinaryAnnotation("lc", name))))
            assert(
              tracer.toSeq.contains(
                Record(childTraceId, startTime, Annotation.Message("local/begin"))))
            assert(
              tracer.toSeq.contains(
                Record(childTraceId, startTime.plus(1.second), Annotation.Message("local/end"))))
            assert(parentTraceId != childTraceId)
        }
      }
    }
  }

  test("trace async local span") {
    val mockTimer = new MockTimer()
    val startTime = Time.now
    Time.withTimeAt(startTime) { ctrl =>
      val tracer = new BufferingTracer()
      val parentTraceId = Trace.nextId
      val name = "work"
      Trace.letTracerAndId(tracer, parentTraceId) {
        val childTraceIdFuture = Trace.traceLocalFuture(name) {
          Future.Done.delayed(1.second)(mockTimer).map(_ => Trace.id)
        }

        ctrl.advance(1.second)
        mockTimer.tick()

        val childTraceId = Await.result(childTraceIdFuture)

        assert(tracer.toSeq.contains(Record(childTraceId, startTime, Annotation.Rpc(name))))
        assert(
          tracer.toSeq.contains(Record(childTraceId, startTime, Annotation.ServiceName("local"))))
        assert(
          tracer.toSeq.contains(
            Record(childTraceId, startTime, Annotation.BinaryAnnotation("lc", name))))
        assert(
          tracer.toSeq.contains(Record(childTraceId, startTime, Annotation.Message("local/begin"))))
        assert(
          tracer.toSeq.contains(
            Record(childTraceId, startTime.plus(1.second), Annotation.Message("local/end"))))
        assert(parentTraceId != childTraceId)
      }
    }
  }

  test("trace async local exceptional span") {
    val mockTimer = new MockTimer()
    val startTime = Time.now
    val name = "work"
    Time.withTimeAt(startTime) { ctrl =>
      val tracer = new BufferingTracer()
      val parentTraceId = Trace.nextId
      Trace.letTracerAndId(tracer, parentTraceId) {
        val childTraceIdFuture = Trace.traceLocalFuture(name) {
          Future.Done
            .delayed(1.second)(mockTimer).flatMap(_ => Future.exception(TraceIdException(Trace.id)))
        }

        ctrl.advance(1.second)
        mockTimer.tick()

        try {
          Await.result(childTraceIdFuture)
          fail("Expected exception to be thrown")
        } catch {
          case TraceIdException(childTraceId) =>
            assert(tracer.toSeq.contains(Record(childTraceId, startTime, Annotation.Rpc(name))))
            assert(
              tracer.toSeq.contains(
                Record(childTraceId, startTime, Annotation.ServiceName("local"))))
            assert(
              tracer.toSeq.contains(
                Record(childTraceId, startTime, Annotation.BinaryAnnotation("lc", name))))
            assert(
              tracer.toSeq.contains(
                Record(childTraceId, startTime, Annotation.Message("local/begin"))))
            assert(
              tracer.toSeq.contains(
                Record(childTraceId, startTime.plus(1.second), Annotation.Message("local/end"))))
            assert(parentTraceId != childTraceId)
        }
      }
    }
  }

  test("time a computation and trace it") {
    val startTime = Time.now
    Time.withTimeAt(startTime) { ctrl =>
      val tracer = new BufferingTracer()
      val traceId = Trace.nextId
      Trace.letTracerAndId(tracer, traceId) {
        Trace.time("duration") {
          ctrl.advance(1.second)
        }

        assert(
          tracer.toSeq.contains(
            Record(traceId, startTime.plus(1.second), BinaryAnnotation("duration", 1.second))))
      }
    }
  }

  test("time an async computation and trace it") {
    val mockTimer = new MockTimer()
    val startTime = Time.now
    Time.withTimeAt(startTime) { ctrl =>
      val tracer = new BufferingTracer()
      val traceId = Trace.nextId
      val result = Trace.letTracerAndId(tracer, traceId) {
        Trace.timeFuture("duration") {
          Future.Done.delayed(1.second)(mockTimer)
        }
      }

      ctrl.advance(1.second)
      mockTimer.tick()

      Await.ready(result)

      assert(
        tracer.toSeq.contains(
          Record(traceId, startTime.plus(1.second), BinaryAnnotation("duration", 1.second))))
    }
  }
}

object TraceTest {
  case class TraceIdException(id: TraceId) extends Exception
}
