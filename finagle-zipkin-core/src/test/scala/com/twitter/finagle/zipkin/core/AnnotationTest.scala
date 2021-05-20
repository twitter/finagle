package com.twitter.finagle.zipkin.core

import com.twitter.util.Time
import org.scalatest.funsuite.AnyFunSuite

class AnnotationTest extends AnyFunSuite {
  test("ZipkinAnnotation should serialize properly") {
    val ann = ZipkinAnnotation(Time.fromSeconds(123), "value", Endpoint(123, 123))
    val tann = ann.toThrift

    assert(tann.isSetHost)
    assert(tann.host.ipv4 == ann.endpoint.ipv4)
    assert(tann.host.port == ann.endpoint.port)
    assert(tann.isSetValue)
    assert(tann.value == ann.value)
    assert(tann.isSetTimestamp)
    assert(tann.timestamp == ann.timestamp.inMicroseconds)
  }
}
