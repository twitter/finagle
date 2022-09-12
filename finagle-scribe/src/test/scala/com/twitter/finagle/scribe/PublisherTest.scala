package com.twitter.finagle.scribe

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.thrift.scribe.thriftscala.LogEntry
import com.twitter.finagle.thrift.scribe.thriftscala.ResultCode
import com.twitter.finagle.thrift.scribe.thriftscala.Scribe
import com.twitter.finagle.ChannelWriteException
import com.twitter.finagle.Service
import com.twitter.util.Await
import com.twitter.util.Awaitable
import com.twitter.util.Future
import java.nio.charset.{StandardCharsets => JChar}
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.concurrent.Eventually

private object PublisherTest {
  val TestArrayBytes: Array[Byte] = "Hello, world".getBytes(JChar.UTF_8)
  val TestLogEntries: Seq[LogEntry] = Seq(
    LogEntry(
      category = "scribe",
      message = "Hello, world\n"
    ))

  class ScribeClient extends Scribe.MethodPerEndpoint {
    var messages: Seq[LogEntry] = Seq.empty[LogEntry]
    var response: Future[ResultCode] = Future.value(ResultCode.Ok)
    def log(msgs: scala.collection.Seq[LogEntry]): Future[ResultCode] = {
      messages ++= msgs
      response
    }
  }
}

class PublisherTest extends AnyFunSuite with MockitoSugar with Eventually {
  import PublisherTest._

  private def await[A](a: Awaitable[A]): A = Await.result(a, 2.seconds)

  test("exhaustively retries TryLater responses -- log entries") {
    val inMemoryStatsReceiver = new InMemoryStatsReceiver()
    val alwaysTryLaterSvc = Service.const(Future.value(ResultCode.TryLater))
    val publisher =
      Publisher.builder
        .withDest("inet!localhost:1234")
        .withStatsReceiver(inMemoryStatsReceiver)
        .withLogServiceOverride(Some(alwaysTryLaterSvc))
        .build(category = "category", label = "foo-publisher")
    await(publisher.write(TestLogEntries))

    assert(inMemoryStatsReceiver.counter("foo-publisher", "scribe", "try_later")() == 3)
    assert(inMemoryStatsReceiver.counter("foo-publisher", "scribe", "ok")() == 0)

    eventually {
      assert(
        inMemoryStatsReceiver.stat("clnt", "foo-publisher", "retries")().map(_.toInt) == Seq(2))
      assert(inMemoryStatsReceiver.counter("clnt", "foo-publisher", "logical", "requests")() == 1)
      assert(inMemoryStatsReceiver.counter("clnt", "foo-publisher", "logical", "success")() == 0)
      assert(inMemoryStatsReceiver.counter("clnt", "foo-publisher", "logical", "failures")() == 1)
      assert(
        inMemoryStatsReceiver.counter(
          "clnt",
          "foo-publisher",
          "logical",
          "failures",
          "com.twitter.finagle.service.ResponseClassificationSyntheticException")() == 1)
    }
  }

  test("exhaustively retries TryLater responses -- array bytes") {
    val inMemoryStatsReceiver = new InMemoryStatsReceiver()
    val alwaysTryLaterSvc = Service.const(Future.value(ResultCode.TryLater))
    val publisher =
      Publisher.builder
        .withDest("inet!localhost:1234")
        .withStatsReceiver(inMemoryStatsReceiver)
        .withLogServiceOverride(Some(alwaysTryLaterSvc))
        .build(category = "category", label = "foo-publisher")
    await(publisher.write(TestArrayBytes))

    assert(inMemoryStatsReceiver.counter("foo-publisher", "scribe", "try_later")() == 3)
    assert(inMemoryStatsReceiver.counter("foo-publisher", "scribe", "ok")() == 0)

    assert(inMemoryStatsReceiver.stat("clnt", "foo-publisher", "retries")().map(_.toInt) == Seq(2))
    assert(inMemoryStatsReceiver.counter("clnt", "foo-publisher", "logical", "requests")() == 1)
    assert(inMemoryStatsReceiver.counter("clnt", "foo-publisher", "logical", "success")() == 0)
    assert(inMemoryStatsReceiver.counter("clnt", "foo-publisher", "logical", "failures")() == 1)
    assert(
      inMemoryStatsReceiver.counter(
        "clnt",
        "foo-publisher",
        "logical",
        "failures",
        "com.twitter.finagle.service.ResponseClassificationSyntheticException")() == 1)
  }

  test("marks unknown result code as non-retryable failures -- log entries") {
    val inMemoryStatsReceiver = new InMemoryStatsReceiver()
    val unknownResultCodeSvc = Service.const(Future.value(ResultCode.EnumUnknownResultCode(100)))
    val publisher =
      Publisher.builder
        .withDest("inet!localhost:1234")
        .withStatsReceiver(inMemoryStatsReceiver)
        .withLogServiceOverride(Some(unknownResultCodeSvc))
        .build(category = "category", label = "foo-publisher")
    await(publisher.write(TestLogEntries))

    assert(
      inMemoryStatsReceiver
        .counter("foo-publisher", "scribe", "error", "EnumUnknownResultCode100")() == 1)
    assert(inMemoryStatsReceiver.counter("foo-publisher", "scribe", "try_later")() == 0)
    assert(inMemoryStatsReceiver.counter("foo-publisher", "scribe", "ok")() == 0)

    assert(inMemoryStatsReceiver.stat("clnt", "foo-publisher", "retries")().map(_.toInt) == Seq(0))
    assert(inMemoryStatsReceiver.counter("clnt", "foo-publisher", "logical", "requests")() == 1)
    assert(inMemoryStatsReceiver.counter("clnt", "foo-publisher", "logical", "success")() == 0)
    assert(inMemoryStatsReceiver.counter("clnt", "foo-publisher", "logical", "failures")() == 1)
    assert(
      inMemoryStatsReceiver.counter(
        "clnt",
        "foo-publisher",
        "logical",
        "failures",
        "com.twitter.finagle.service.ResponseClassificationSyntheticException")() == 1)
  }

  test("marks unknown result code as non-retryable failures -- array bytes") {
    val inMemoryStatsReceiver = new InMemoryStatsReceiver()
    val unknownResultCodeSvc = Service.const(Future.value(ResultCode.EnumUnknownResultCode(100)))
    val publisher =
      Publisher.builder
        .withDest("inet!localhost:1234")
        .withStatsReceiver(inMemoryStatsReceiver)
        .withLogServiceOverride(Some(unknownResultCodeSvc))
        .build(category = "category", label = "foo-publisher")
    await(publisher.write(TestArrayBytes))

    assert(
      inMemoryStatsReceiver
        .counter("foo-publisher", "scribe", "error", "EnumUnknownResultCode100")() == 1)
    assert(inMemoryStatsReceiver.counter("foo-publisher", "scribe", "try_later")() == 0)
    assert(inMemoryStatsReceiver.counter("foo-publisher", "scribe", "ok")() == 0)

    assert(inMemoryStatsReceiver.stat("clnt", "foo-publisher", "retries")().map(_.toInt) == Seq(0))
    assert(inMemoryStatsReceiver.counter("clnt", "foo-publisher", "logical", "requests")() == 1)
    assert(inMemoryStatsReceiver.counter("clnt", "foo-publisher", "logical", "success")() == 0)
    assert(inMemoryStatsReceiver.counter("clnt", "foo-publisher", "logical", "failures")() == 1)
    assert(
      inMemoryStatsReceiver.counter(
        "clnt",
        "foo-publisher",
        "logical",
        "failures",
        "com.twitter.finagle.service.ResponseClassificationSyntheticException")() == 1)
  }

  test("does not retry failures handled by the RequeueFilter -- log entries") {
    val inMemoryStatsReceiver = new InMemoryStatsReceiver()
    val failingLogSvc = Service.const(Future.exception(new ChannelWriteException(None)))
    val publisher =
      Publisher.builder
        .withDest("inet!localhost:1234")
        .withStatsReceiver(inMemoryStatsReceiver)
        .withLogServiceOverride(Some(failingLogSvc))
        .build(category = "category", label = "foo-publisher")
    intercept[ChannelWriteException] {
      await(publisher.write(TestLogEntries))
    }

    assert(
      inMemoryStatsReceiver.counter(
        "foo-publisher",
        "scribe",
        "error",
        "com.twitter.finagle.ChannelWriteException")() == 1)
    assert(inMemoryStatsReceiver.counter("foo-publisher", "scribe", "try_later")() == 0)
    assert(inMemoryStatsReceiver.counter("foo-publisher", "scribe", "ok")() == 0)

    assert(inMemoryStatsReceiver.stat("clnt", "foo-publisher", "retries")().map(_.toInt) == Seq(0))
    assert(inMemoryStatsReceiver.counter("clnt", "foo-publisher", "logical", "requests")() == 1)
    assert(inMemoryStatsReceiver.counter("clnt", "foo-publisher", "logical", "success")() == 0)
    assert(inMemoryStatsReceiver.counter("clnt", "foo-publisher", "logical", "failures")() == 1)
    assert(
      inMemoryStatsReceiver.counter(
        "clnt",
        "foo-publisher",
        "logical",
        "failures",
        "com.twitter.finagle.ChannelWriteException")() == 1)
  }

  test("does not retry failures handled by the RequeueFilter -- array bytes") {
    val inMemoryStatsReceiver = new InMemoryStatsReceiver()
    val failingLogSvc = Service.const(Future.exception(new ChannelWriteException(None)))
    val publisher =
      Publisher.builder
        .withDest("inet!localhost:1234")
        .withStatsReceiver(inMemoryStatsReceiver)
        .withLogServiceOverride(Some(failingLogSvc))
        .build(category = "category", label = "foo-publisher")
    intercept[ChannelWriteException] {
      await(publisher.write(TestArrayBytes))
    }

    assert(
      inMemoryStatsReceiver.counter(
        "foo-publisher",
        "scribe",
        "error",
        "com.twitter.finagle.ChannelWriteException")() == 1)
    assert(inMemoryStatsReceiver.counter("foo-publisher", "scribe", "try_later")() == 0)
    assert(inMemoryStatsReceiver.counter("foo-publisher", "scribe", "ok")() == 0)

    assert(inMemoryStatsReceiver.stat("clnt", "foo-publisher", "retries")().map(_.toInt) == Seq(0))
    assert(inMemoryStatsReceiver.counter("clnt", "foo-publisher", "logical", "requests")() == 1)
    assert(inMemoryStatsReceiver.counter("clnt", "foo-publisher", "logical", "success")() == 0)
    assert(inMemoryStatsReceiver.counter("clnt", "foo-publisher", "logical", "failures")() == 1)
    assert(
      inMemoryStatsReceiver.counter(
        "clnt",
        "foo-publisher",
        "logical",
        "failures",
        "com.twitter.finagle.ChannelWriteException")() == 1)
  }

  test("reports correct metrics after a successful retry -- log entries") {
    val inMemoryStatsReceiver = new InMemoryStatsReceiver()
    val tryLaterThenOkSvc = mock[Service[Scribe.Log.Args, ResultCode]]
    when(tryLaterThenOkSvc.apply(any[Scribe.Log.Args]))
      .thenReturn(Future.value(ResultCode.TryLater))
      .thenReturn(Future.value(ResultCode.Ok))

    val publisher =
      Publisher.builder
        .withDest("inet!localhost:1234")
        .withStatsReceiver(inMemoryStatsReceiver)
        .withLogServiceOverride(Some(tryLaterThenOkSvc))
        .build(category = "category", label = "foo-publisher")
    await(publisher.write(TestLogEntries))

    assert(inMemoryStatsReceiver.counter("foo-publisher", "scribe", "try_later")() == 1)
    assert(inMemoryStatsReceiver.counter("foo-publisher", "scribe", "ok")() == 1)

    assert(inMemoryStatsReceiver.stat("clnt", "foo-publisher", "retries")().map(_.toInt) == Seq(1))
    assert(inMemoryStatsReceiver.counter("clnt", "foo-publisher", "logical", "requests")() == 1)
    assert(inMemoryStatsReceiver.counter("clnt", "foo-publisher", "logical", "success")() == 1)
  }

  test("reports correct metrics after a successful retry -- array bytes") {
    val inMemoryStatsReceiver = new InMemoryStatsReceiver()
    val tryLaterThenOkSvc = mock[Service[Scribe.Log.Args, ResultCode]]
    when(tryLaterThenOkSvc.apply(any[Scribe.Log.Args]))
      .thenReturn(Future.value(ResultCode.TryLater))
      .thenReturn(Future.value(ResultCode.Ok))

    val publisher =
      Publisher.builder
        .withDest("inet!localhost:1234")
        .withStatsReceiver(inMemoryStatsReceiver)
        .withLogServiceOverride(Some(tryLaterThenOkSvc))
        .build(category = "category", label = "foo-publisher")
    await(publisher.write(TestArrayBytes))

    assert(inMemoryStatsReceiver.counter("foo-publisher", "scribe", "try_later")() == 1)
    assert(inMemoryStatsReceiver.counter("foo-publisher", "scribe", "ok")() == 1)

    assert(inMemoryStatsReceiver.stat("clnt", "foo-publisher", "retries")().map(_.toInt) == Seq(1))
    assert(inMemoryStatsReceiver.counter("clnt", "foo-publisher", "logical", "requests")() == 1)
    assert(inMemoryStatsReceiver.counter("clnt", "foo-publisher", "logical", "success")() == 1)
  }

  test("reports errors -- log entries") {
    val inMemoryStatsReceiver = new InMemoryStatsReceiver()
    val failingLogSvc = Service.const(Future.exception(new IllegalArgumentException))
    val publisher =
      Publisher.builder
        .withDest("inet!localhost:1234")
        .withStatsReceiver(inMemoryStatsReceiver)
        .withLogServiceOverride(Some(failingLogSvc))
        .build(category = "category", label = "foo-publisher")
    intercept[IllegalArgumentException] {
      await(publisher.write(TestLogEntries))
    }

    assert(inMemoryStatsReceiver.counter("foo-publisher", "scribe", "try_later")() == 0)
    assert(inMemoryStatsReceiver.counter("foo-publisher", "scribe", "ok")() == 0)
    assert(
      inMemoryStatsReceiver
        .counter("foo-publisher", "scribe", "error", "java.lang.IllegalArgumentException")() == 1)

    assert(inMemoryStatsReceiver.stat("clnt", "foo-publisher", "retries")().map(_.toInt) == Seq(0))
    assert(inMemoryStatsReceiver.counter("clnt", "foo-publisher", "logical", "requests")() == 1)
    assert(inMemoryStatsReceiver.counter("clnt", "foo-publisher", "logical", "success")() == 0)
    assert(inMemoryStatsReceiver.counter("clnt", "foo-publisher", "logical", "failures")() == 1)
    assert(
      inMemoryStatsReceiver.counter(
        "clnt",
        "foo-publisher",
        "logical",
        "failures",
        "java.lang.IllegalArgumentException")() == 1)
  }

  test("reports errors -- array bytes") {
    val inMemoryStatsReceiver = new InMemoryStatsReceiver()
    val failingLogSvc = Service.const(Future.exception(new IllegalArgumentException))
    val publisher =
      Publisher.builder
        .withDest("inet!localhost:1234")
        .withStatsReceiver(inMemoryStatsReceiver)
        .withLogServiceOverride(Some(failingLogSvc))
        .build(category = "category", label = "foo-publisher")
    intercept[IllegalArgumentException] {
      await(publisher.write(TestArrayBytes))
    }

    assert(inMemoryStatsReceiver.counter("foo-publisher", "scribe", "try_later")() == 0)
    assert(inMemoryStatsReceiver.counter("foo-publisher", "scribe", "ok")() == 0)
    assert(
      inMemoryStatsReceiver
        .counter("foo-publisher", "scribe", "error", "java.lang.IllegalArgumentException")() == 1)

    assert(inMemoryStatsReceiver.stat("clnt", "foo-publisher", "retries")().map(_.toInt) == Seq(0))
    assert(inMemoryStatsReceiver.counter("clnt", "foo-publisher", "logical", "requests")() == 1)
    assert(inMemoryStatsReceiver.counter("clnt", "foo-publisher", "logical", "success")() == 0)
    assert(inMemoryStatsReceiver.counter("clnt", "foo-publisher", "logical", "failures")() == 1)
    assert(
      inMemoryStatsReceiver.counter(
        "clnt",
        "foo-publisher",
        "logical",
        "failures",
        "java.lang.IllegalArgumentException")() == 1)
  }

  test("formulate scribe log message correctly from array bytes") {
    val scribe = new ScribeClient
    val publisher = new Publisher("scribe", ScribeStats.Empty, scribe)

    val message: Array[Byte] = "Hello, world".getBytes(JChar.UTF_8)
    val expected = LogEntry(
      category = "scribe",
      message = "Hello, world\n"
    )

    publisher.write(message)
    assert(scribe.messages == Seq(expected))
  }
}
