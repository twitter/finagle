package com.twitter.finagle.b3.thrift

import java.nio.ByteBuffer
import com.twitter.util.Time
import com.twitter.finagle.thrift.thrift

/**
 * Annotation for a span. An event that happened at a particular time at a particular node.
 */
case class B3Annotation(
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

case class B3BinaryAnnotation(
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
