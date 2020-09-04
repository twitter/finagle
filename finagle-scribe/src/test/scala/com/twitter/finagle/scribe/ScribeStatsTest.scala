package com.twitter.finagle.scribe

import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.thrift.scribe.thriftscala.ResultCode
import com.twitter.util.{Return, Throw}
import org.scalatest.funsuite.AnyFunSuite

class ScribeStatsTest extends AnyFunSuite {

  test("records an unexpected ResultCode enum") {
    val sr = new InMemoryStatsReceiver()
    val stats = new ScribeStats(sr)
    stats.respond(Return(ResultCode.EnumUnknownResultCode(100)))

    assert(sr.counter("scribe", "error", "EnumUnknownResultCode100")() == 1)
  }

  test("flattens wrapped throwables when handling errors") {
    val sr = new InMemoryStatsReceiver()
    val stats = new ScribeStats(sr)

    stats.respond(Throw(new RuntimeException(new IllegalArgumentException)))
    stats.handleError(new RuntimeException(new IllegalArgumentException))

    assert(
      sr.counter(
        "scribe",
        "error",
        "java.lang.RuntimeException",
        "java.lang.IllegalArgumentException")() == 2)
  }

  test("records TryLater") {
    val sr = new InMemoryStatsReceiver()
    val stats = new ScribeStats(sr)
    stats.respond(Return(ResultCode.TryLater))

    assert(sr.counter("scribe", "try_later")() == 1)
  }

  test("records Ok") {
    val sr = new InMemoryStatsReceiver()
    val stats = new ScribeStats(sr)
    stats.respond(Return(ResultCode.Ok))

    assert(sr.counter("scribe", "ok")() == 1)
  }
}
