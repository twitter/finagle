package com.twitter.finagle.tracing

import org.specs.Specification
import java.nio.ByteBuffer
import scala.collection.Map

object SpanIdSpec extends Specification {
  "SpanId" should {
    "create a span with the ID 123 from hex '7b'" in {
      SpanId.fromString("7b").get.toLong mustEqual 123L
    }

    "return None if string is not valid hex" in {
      SpanId.fromString("rofl") must beNone
    }

    "return None for overflowed hex" in {
      SpanId.fromString("8000000000000000") must beNone
    }

    "represent a span with the ID 123 as the hex '000000000000007b'" in {
      SpanId(123L).toString mustEqual "000000000000007b" // padded for lexical ordering
    }

    "be equal if the underlying value is equal" in {
      val a = SpanId(1234L)
      val b = SpanId(1234L)

      a mustEqual b
    }
  }
}