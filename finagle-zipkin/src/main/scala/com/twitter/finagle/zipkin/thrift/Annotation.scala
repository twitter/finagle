package com.twitter.finagle.zipkin.thrift

import com.twitter.finagle.thrift.thrift
import com.twitter.util.Time
import java.nio.ByteBuffer

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
    val localEndpoint = endpoint.boundEndpoint.toThrift
    thriftAnnotation.setTimestamp(timestamp.inMicroseconds)
    thriftAnnotation.setValue(value)
    thriftAnnotation.setHost(localEndpoint)

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
    val localEndpoint = endpoint.boundEndpoint.toThrift
    thriftAnnotation.setKey(key)
    thriftAnnotation.setValue(value)
    thriftAnnotation.setAnnotation_type(annotationType)
    thriftAnnotation.setHost(localEndpoint)

    thriftAnnotation
  }
}
