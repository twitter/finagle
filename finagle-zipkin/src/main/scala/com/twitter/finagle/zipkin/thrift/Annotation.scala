package com.twitter.finagle.zipkin.thrift

import java.nio.ByteBuffer
import com.twitter.util.Time
import com.twitter.finagle.thrift.thrift

/**
 * Annotation for a span. An event that happened at a particular time at a particular node.
 */
case class ZipkinAnnotation(
  timestamp: Time,
  value:     String,
  endpoint:  Endpoint
) {

  def toThrift: thrift.Annotation = {
    val thriftAnnotation = new thrift.Annotation
    thriftAnnotation.setTimestamp(timestamp.inMicroseconds.toLong)
    thriftAnnotation.setValue(value)

    endpoint.toThrift foreach { thriftAnnotation.setHost(_) }

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
