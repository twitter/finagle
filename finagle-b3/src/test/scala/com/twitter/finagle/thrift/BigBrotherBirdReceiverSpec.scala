package com.twitter.finagle.thrift

import java.net.ServerSocket
import java.util.logging
import java.util.concurrent.CyclicBarrier

import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.Matchers._

import org.apache.thrift.transport.{TServerSocket, TFramedTransport, TTransportFactory}
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.server.TSimpleServer
import org.apache.thrift.async.AsyncMethodCallback

import com.twitter.test.{B, AnException, SomeStruct}
import com.twitter.util.{RandomSocket, Promise, Return, Throw, Future}

import com.twitter.finagle.{Codec, ClientCodec}
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.tracing.{Span, SpanId, Endpoint}

import java.util.ArrayList

import org.apache.scribe.{ResultCode, LogEntry, scribe}
import com.twitter.finagle.stats.NullStatsReceiver

object BigBrotherBirdReceiverSpec extends Specification with Mockito {
  val span = Span(Some(SpanId(123)), Some(SpanId(456)), Some(SpanId(789)), Some("service"),
    Some("method"), Some(Endpoint(123, 1000)))

  "BigBrotherBirdReceiver" should {
    "throw exception if illegal sample rate" in {
      val receiver = new BigBrotherBirdReceiver(null, NullStatsReceiver)
      receiver.setSampleRate(-1) must throwA[IllegalArgumentException]
      receiver.setSampleRate(10001) must throwA[IllegalArgumentException]
    }

    "not sample any traces and send all to scribe" in {
      val client = mock[scribe.ServiceToClient]

      val receiver = new BigBrotherBirdReceiver(client, NullStatsReceiver)
      receiver.setSampleRate(10000)

      val expected = new ArrayList[LogEntry]()
      expected.add(new LogEntry().setCategory("b3")
        .setMessage("CgABAAAAAAAAAHsLAAIAAAAHc2VydmljZQsAAwAAAAZtZXRob2QKAAQAAAAAAAAByAoABQAAAAAA\nAAMVDQAHCwsAAAAAAA=="))
      client.Log(expected) returns Future(ResultCode.OK)

      // execute the code we're testing
      receiver.receiveSpan(span)

      there was one(client).Log(expected)
    }

    "sample all traces and send none to scribe" in {
      val client = mock[scribe.ServiceToClient]

      val receiver = new BigBrotherBirdReceiver(client, NullStatsReceiver)
      receiver.setSampleRate(0)
      receiver.receiveSpan(span)

      there was no(client).Log(any[ArrayList[LogEntry]])
    }
  }
}