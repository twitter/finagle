package com.twitter.finagle.tracing

import org.specs.Specification

object TracerSpec extends Specification {
  "RefTracer" should {
    val init = Span().copy(id = 321L)
    val tracer = new RootRefTracer(init)

    "perform mutations on the underlying span" in {
      tracer.mutate { _.copy(id = 123L) }
      tracer().id must be_==(123L)
    }

    "record children, propagating its changes" in {
      val child = tracer.addChild()
      child.record("oh hey ho yay")
      child.record("another")

      tracer().children must beLike {
        case Seq(
          Span(init._traceId, _, _, _, Some(init.id),
               Seq(Annotation(_, Event.Message("oh hey ho yay"), _),
                   Annotation(_, Event.Message("another"), _)),
               Seq())) => true
        case _ =>
          false
      }
    }
  }
}
