package com.twitter.finagle.demo

import java.util.concurrent.atomic.AtomicInteger
import java.net.InetSocketAddress

import com.twitter.util.Future

import org.apache.thrift.protocol.TBinaryProtocol

import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.thrift.{ThriftServerFramedCodec, ThriftClientFramedCodec}
import com.twitter.finagle.tracing.{Trace, BufferingTranscript}

object Tracing1Service extends Tracing1.ServiceIface {
  private[this] val transport = ClientBuilder()
    .hosts("localhost:6002")
    .codec(ThriftClientFramedCodec())
    .build()

  private[this] val t2Client =
    new Tracing2.ServiceToClient(transport, new TBinaryProtocol.Factory())

  def main(args: Array[String]) {
    ServerBuilder()
      .codec(ThriftServerFramedCodec())
      .bindTo(new InetSocketAddress(6001))
      .build(new Tracing1.Service(this, new TBinaryProtocol.Factory()))    
  } 

  def computeSomething(): Future[String] = {
    println("T1 with trace ID", Trace().traceID)
    Trace.record("hey i'm issuing a call")

    t2Client.computeSomethingElse() map { somethingElse =>
      "t1: " + somethingElse
    }
  }
}

object Tracing2Service extends Tracing2.ServiceIface {
  private[this] val transport = ClientBuilder()
    .hosts("localhost:6003")
    .codec(ThriftClientFramedCodec())
    .build()

  private[this] val t3Client =
    new Tracing3.ServiceToClient(transport, new TBinaryProtocol.Factory())

  def main(args: Array[String]) {
    ServerBuilder()
      .codec(ThriftServerFramedCodec())
      .bindTo(new InetSocketAddress(6002))
      .build(new Tracing2.Service(this, new TBinaryProtocol.Factory()))
  }

  def computeSomethingElse(): Future[String] = {
    println("T2 with trace ID", Trace().traceID)
    Trace.record("hey i'm issuing a call")

    for {
      x <- t3Client.oneMoreThingToCompute()
      y <- t3Client.oneMoreThingToCompute()
    } yield {
      Trace.record(
        "got my results!  (%s and %s), returning".format(x, y))
      "t2: " + x + y
    }
  }
}

object Tracing3Service extends Tracing3.ServiceIface {
  private[this] val count = new AtomicInteger(0)

  def main(args: Array[String]) {
    ServerBuilder()
      .codec(ThriftServerFramedCodec())
      .bindTo(new InetSocketAddress(6003))
      .build(new Tracing3.Service(this, new TBinaryProtocol.Factory()))
  }

  def oneMoreThingToCompute(): Future[String] = {
    println("T3 with trace ID", Trace().traceID)

    val number = count.incrementAndGet()
    Trace.record(
      "(t3) hey i'm issuing a call %s".format(number))
    Future("t3: %d".format(number))
  }
}

object Client {
  def main(args: Array[String]) {
    val transport = ClientBuilder()
      .hosts("localhost:6001")
      .codec(ThriftClientFramedCodec())
      .build()

    val client = new Tracing1.ServiceToClient(
      transport, new TBinaryProtocol.Factory())

    // Turn (debug) tracing on.
    Trace().transcript = new BufferingTranscript(Trace().traceID)

    Trace.record("about to start issuing the root request..")
    val result = client.computeSomething()()
    println("result", result)

    println("Trace:")
    Trace().transcript.print()
  }
}
