package com.twitter.finagle.b3.thrift

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

  def toThrift(): thrift.Annotation = {
    val thriftAnnotation = new thrift.Annotation
    thriftAnnotation.setTimestamp(timestamp.inMicroseconds.toLong)
    thriftAnnotation.setValue(value)

    if (endpoint != Endpoint.Unknown) {
      val e = new thrift.Endpoint
      e.setIpv4(endpoint.ipv4)
      e.setPort(endpoint.port)
      thriftAnnotation.setHost(e)
    }
    thriftAnnotation
  }

}