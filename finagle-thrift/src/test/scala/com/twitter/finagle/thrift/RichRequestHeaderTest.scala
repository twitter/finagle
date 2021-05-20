package com.twitter.finagle.thrift

import com.twitter.finagle.{Path, Dtab}
import com.twitter.finagle.tracing.{Flags, SpanId, TraceId}
import java.util.ArrayList
import org.scalatest.funsuite.AnyFunSuite

class RichRequestHeaderTest extends AnyFunSuite {
  test("None if clientId is not set") {
    val header = new thrift.RequestHeader
    val richHeader = new RichRequestHeader(header)
    assert(None == richHeader.clientId)
  }

  test("None if clientId.name is not set") {
    val header = (new thrift.RequestHeader)
      .setClient_id(new thrift.ClientId)

    val richHeader = new RichRequestHeader(header)
    assert(None == richHeader.clientId)
  }

  test("Some(clientId)") {
    val header = (new thrift.RequestHeader)
      .setClient_id(new thrift.ClientId("foo"))

    val richHeader = new RichRequestHeader(header)
    assert(Some(ClientId("foo")) == richHeader.clientId)
  }

  test("empth path if dest is null") {
    val header = new thrift.RequestHeader
    val richHeader = new RichRequestHeader(header)
    assert(Path.empty == richHeader.dest)
  }

  test("path if dest is non-null") {
    val header = (new thrift.RequestHeader)
      .setDest("/foo")

    val richHeader = new RichRequestHeader(header)
    assert(Path.read("/foo") == richHeader.dest)
  }

  test("null dtab") {
    val header = new thrift.RequestHeader
    val richHeader = new RichRequestHeader(header)
    assert(Dtab.empty == richHeader.dtab)
  }

  test("non-null dtab") {
    val delegations = new ArrayList[thrift.Delegation]
    delegations.add(new thrift.Delegation("/foo", "/bar"))

    val header = (new thrift.RequestHeader)
      .setDelegations(delegations)

    val richHeader = new RichRequestHeader(header)
    assert(Dtab.read("/foo=>/bar") == richHeader.dtab)
  }

  test("default traceId") {
    val header = new thrift.RequestHeader
    val richHeader = new RichRequestHeader(header)
    assert(TraceId(Some(SpanId(0)), None, SpanId(0), None, Flags()) == richHeader.traceId)
  }

  test("non-default traceId") {
    val header = (new thrift.RequestHeader)
      .setTrace_id(0)
      .setParent_span_id(1)
      .setSpan_id(2)
      .setSampled(true)
      .setFlags(4)

    val richHeader = new RichRequestHeader(header)
    val expected = TraceId(Some(SpanId(0)), Some(SpanId(1)), SpanId(2), Some(true), Flags(4))
    assert(expected == richHeader.traceId)
  }
}
