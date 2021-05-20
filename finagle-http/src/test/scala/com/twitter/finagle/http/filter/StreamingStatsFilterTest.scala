package com.twitter.finagle.http.filter

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Service
import com.twitter.finagle.http.{Method, Request, Response, Status, Version}
import com.twitter.finagle.stats.{CategorizingExceptionStatsHandler, InMemoryStatsReceiver}
import com.twitter.io.{Buf, Pipe, Reader}
import com.twitter.util.{Await, Future, Stopwatch, Time}
import org.scalatest.funsuite.AnyFunSuite

class StreamingStatsFilterTest extends AnyFunSuite {

  def await[T](f: Future[T]): T = Await.result(f, 5.seconds)

  val streamingService = new Service[Request, Response] {
    def apply(request: Request): Future[Response] = {
      val response = Response()
      response.setChunked(true)
      response.statusCode = 200
      Future.value(response)
    }
  }

  val failedStreamingService = new Service[Request, Response] {
    def apply(request: Request): Future[Response] = {
      // create a failed reader
      val pipe = new Pipe[Buf]
      pipe.write(Buf.Utf8("1"))
      val reader = Reader.flatten(Reader.value(pipe))
      reader.read()
      pipe.fail(new Exception("boom"))
      // put the failed reader in the response
      val response = Response(Version.Http11, Status.InternalServerError, reader)
      intercept[Exception] {
        await(response.reader.onClose)
      }
      Future.value(response)
    }
  }

  test("streaming failures in request are populated correctly") {
    val receiver = new InMemoryStatsReceiver
    val exceptionStatsHandler = new CategorizingExceptionStatsHandler
    // create a failed reader
    val pipe = new Pipe[Buf]
    pipe.write(Buf.Utf8("1"))
    val reader = Reader.flatten(Reader.value(pipe))
    reader.read()
    pipe.fail(new Exception("boom"))
    // put the failed reader in the request
    val streamingRequest = Request(Version.Http11, Method.Post, "/", reader)
    intercept[Exception] {
      await(streamingRequest.reader.onClose)
    }

    val filteredService =
      new StreamingStatsFilter(receiver, exceptionStatsHandler) andThen streamingService

    await(filteredService(streamingRequest))

    assert(receiver.counters(Seq("stream", "request", "failures", "java.lang.Exception")) == 1L)
    assert(receiver.counters(Seq("stream", "request", "failures")) == 1)
    assert(!receiver.counters.contains(Seq("stream", "response", "failures")))
  }

  test("streaming failures in response are populated correctly") {
    val receiver = new InMemoryStatsReceiver
    val exceptionStatsHandler = new CategorizingExceptionStatsHandler
    val streamingRequest = Request()

    val filteredService =
      new StreamingStatsFilter(receiver, exceptionStatsHandler) andThen failedStreamingService

    await(filteredService(streamingRequest))

    assert(receiver.counters(Seq("stream", "response", "failures", "java.lang.Exception")) == 1L)
    assert(receiver.counters(Seq("stream", "response", "failures")) == 1)
    assert(!receiver.counters.contains(Seq("stream", "request", "failures")))
  }

  test("opened stream count and finished stream count are populated correctly") {
    val receiver = new InMemoryStatsReceiver
    val exceptionStatsHandler = new CategorizingExceptionStatsHandler
    val streamingRequest1 = Request()
    streamingRequest1.setChunked(true)
    val streamingRequest2 = Request()
    streamingRequest2.setChunked(true)

    val filteredService =
      new StreamingStatsFilter(receiver, exceptionStatsHandler) andThen streamingService

    val response1 = await(filteredService(streamingRequest1))

    response1.reader.discard()
    streamingRequest1.reader.discard()

    val response2 = await(filteredService(streamingRequest2))

    response2.reader.discard()

    assert(receiver.counters(Seq("stream", "request", "opened")) == 2L)
    assert(receiver.counters(Seq("stream", "request", "closed")) == 1L)
    assert(receiver.counters(Seq("stream", "response", "opened")) == 2L)
    assert(receiver.counters(Seq("stream", "response", "closed")) == 2L)
  }

  test("pending stream gauges are populated correctly") {
    val receiver = new InMemoryStatsReceiver
    val exceptionStatsHandler = new CategorizingExceptionStatsHandler
    val streamingRequest1 = Request()
    val streamingRequest2 = Request()
    streamingRequest1.setChunked(true)
    streamingRequest2.setChunked(true)

    val filteredService =
      new StreamingStatsFilter(receiver, exceptionStatsHandler).andThen(streamingService)

    val response1 = await(filteredService(streamingRequest1))

    await(filteredService(streamingRequest2))

    assert(receiver.gauges(Seq("stream", "request", "pending"))() == 2.0)
    assert(receiver.gauges(Seq("stream", "response", "pending"))() == 2.0)

    // only close request#1 and response#1
    response1.reader.discard()
    streamingRequest1.reader.discard()

    assert(receiver.gauges(Seq("stream", "request", "pending"))() == 1.0)
    assert(receiver.gauges(Seq("stream", "response", "pending"))() == 1.0)
  }

  test("stream duration stats are populated correctly") {
    val receiver = new InMemoryStatsReceiver
    val exceptionStatsHandler = new CategorizingExceptionStatsHandler
    val streamingRequest = Request()
    streamingRequest.setChunked(true)

    val filteredService = new StreamingStatsFilter(
      receiver,
      exceptionStatsHandler,
      Stopwatch.timeMillis) andThen streamingService

    Time.withCurrentTimeFrozen { timeControl =>
      val response = await(filteredService(streamingRequest))
      timeControl.advance(2.seconds)
      response.reader.discard()
      streamingRequest.reader.discard()
      assert(
        receiver
          .stats(Seq("stream", "request", "duration_ms")).head.toLong == 2.seconds.inMilliseconds)
      assert(
        receiver
          .stats(Seq("stream", "response", "duration_ms")).head.toLong == 2.seconds.inMilliseconds)
    }
  }
}
