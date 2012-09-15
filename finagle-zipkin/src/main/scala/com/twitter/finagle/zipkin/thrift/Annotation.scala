package com.twitter.finagle.zipkin.thrift

import com.twitter.finagle.thrift.thrift
import com.twitter.util.{Duration, Time}
import java.nio.ByteBuffer

/**
 * Annotation for a span. An event that happened at a particular time at a particular node.
 */
case class ZipkinAnnotation(
  timestamp: Time,
  value:     String,
  endpoint:  Endpoint,
  duration:  Option[Duration]
) {

  def toThrift: thrift.Annotation = {
    val thriftAnnotation = new thrift.Annotation
    thriftAnnotation.setTimestamp(timestamp.inMicroseconds)
    thriftAnnotation.setValue(value)

    endpoint.toThrift foreach { thriftAnnotation.setHost(_) }
    duration foreach { d => thriftAnnotation.setDuration(d.inMicroseconds.toInt) }

    thriftAnnotation
  }
}

case class BinaryAnnotation(
  key: String,
  value: ByteBuffer,
  annotationType: thrift.AnnotationType,
  endpoint: Endpoint
) {
  def toThrift: thrift.BinaryAnnotation = {
    val thriftAnnotation = new thrift.BinaryAnnotation
    thriftAnnotation.setKey(key)
    thriftAnnotation.setValue(value)
    thriftAnnotation.setAnnotation_type(annotationType)

    endpoint.toThrift foreach { thriftAnnotation.setHost(_) }

    thriftAnnotation
  }
}
