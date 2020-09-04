package com.twitter.finagle.scribe

import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.thrift.scribe.thriftscala.ResultCode
import com.twitter.util.{Return, Throw, Throwables, Try}

object ScribeStats {
  private val Label = "scribe"
  private[finagle] val Empty = new ScribeStats(NullStatsReceiver)
}

/**
 * A helper class to organize metrics related to Scribe.
 *
 * Metrics will be in the form:
 * {{{
 *   scribe/try_later
 *   scribe/ok
 *   scribe/error/java.lang.RuntimeException
 *   scribe/error/EnumUnknownResultCode100
 * }}}
 */
private[finagle] class ScribeStats(statsReceiver: StatsReceiver) {
  import ScribeStats._

  private[this] val scoped = statsReceiver.scope(Label)
  // everything is lazy to ensure we do not eagerly create
  private[this] lazy val tryLaterCounter = scoped.counter("try_later")
  private[this] lazy val okCounter = scoped.counter("ok")
  private[this] lazy val errorReceiver = scoped.scope("error")

  /**
   * Report errors that occur before hitting the wire
   */
  val handleError: Throwable => Unit = { e: Throwable =>
    errorReceiver.counter(Throwables.mkString(e): _*).incr()
  }

  /**
   * Report metrics from Scribe's response.
   */
  val respond: Try[ResultCode] => Unit = {
    case Return(ResultCode.Ok) =>
      okCounter.incr()
    case Return(ResultCode.TryLater) =>
      tryLaterCounter.incr()
    case Return(e: ResultCode.EnumUnknownResultCode) =>
      errorReceiver.counter(e.name).incr()
    case Throw(e) =>
      errorReceiver.counter(Throwables.mkString(e): _*).incr()
  }
}
