package com.twitter.finagle.tracing

import org.specs.Specification
import scala.collection.Map

import java.nio.ByteBuffer

object TraceSpec extends Specification {
  "Trace" should {
    "start and end spans" in {
      Trace.startSpan()
      Trace.debug(true)
      Trace.record(Event.ClientSend())
      Trace.record("oh hey")
      val span = Trace.endSpan()
      val emptyMap = Map[String, ByteBuffer]()

      span must beLike {
        case Span(
          None, None, None, _, None,
          Seq(Annotation(_, Event.ClientSend(), _),
              Annotation(_, Event.Message("oh hey"), _)),
          emptyMap,
          Seq()) => true
        case _ => false
      }

      Trace().annotations must beEmpty
    }

    "add child spans, updating the parent span" in {
      Trace.startSpan()
      Trace.record(Event.ClientSend())
      val child = Trace.addChild
      child.record(Event.ClientRecv())
      val span = Trace.endSpan()
      val emptyMap1 = Map[String, ByteBuffer]()
      val emptyMap2 = Map[String, ByteBuffer]()

      span must beLike {
        case Span(
          None, None, None, _, None,
          Seq(Annotation(_, Event.ClientSend(), _)),
          emptyMap1,
          Seq(
            Span(None, None, None, _, Some(span.id),
                 Seq(Annotation(_, Event.ClientRecv(), _)),
                 emptyMap2,
                 Seq()))) => true
        case _ => false
      }
    }
  }
}
