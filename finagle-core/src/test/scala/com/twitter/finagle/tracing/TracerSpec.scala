package com.twitter.finagle.tracing

import org.specs.Specification
import java.nio.ByteBuffer
import scala.collection.Map

object TracerSpec extends Specification {
  "RefTracer" should {
    val init = Span().copy(id = SpanId(321L))
    val tracer = new RootRefTracer(init)

    "perform mutations on the underlying span" in {
      tracer.mutate { _.copy(id = SpanId(123L)) }
      tracer().id must be_==(SpanId(123L))
    }

    "record children, propagating its changes" in {
      tracer.debug(true)
      val child = tracer.addChild(Some("service"), Some("method"), Some(Endpoint.Unknown))
      child.record("oh hey ho yay")
      child.record("another")
      child.recordBinary("this is a key", ByteBuffer.wrap("this is a value".getBytes()))

      val testMap = Map("this is a key" -> ByteBuffer.wrap("this is a value".getBytes()))
      tracer().children must beLike {
        case Seq(
          Span(init._traceId, _, _, _, Some(init.id),
               Seq(Annotation(_, Event.Message("oh hey ho yay"), _),
                   Annotation(_, Event.Message("another"), _)),
               testMap,
               Some(Endpoint.Unknown),
               Seq())) => true
        case _ =>
          false
      }
    }

    "not record messages when debugging isn't turned on" in {
      tracer.record("oh hey")
      tracer.recordBinary("this is a key", ByteBuffer.wrap("this is a value".getBytes()))
      tracer().annotations must beEmpty
      tracer().bAnnotations must beEmpty
    }
  }
}
