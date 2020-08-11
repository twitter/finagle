package com.twitter.finagle.zipkin.thrift

import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.thrift.scribe.thriftscala.ResultCode
import com.twitter.util.{Return, Throw, Throwables, Try}

private[thrift] object ScribeStats {
  val label = "scribe"
  val empty = new ScribeStats(NullStatsReceiver)
}

/**
 * A helper class to organize metrics related to Scribe
 */
private[thrift] class ScribeStats(statsReceiver: StatsReceiver) {
  import ScribeStats._

  private[this] val scoped = statsReceiver.scope(label)
  private[this] val tryLaterCounter = scoped.counter("try_later")
  private[this] val okCounter = scoped.counter("ok")
  private[this] val errorReceiver = scoped.scope("error")

  /**
   * Report errors that occur before hitting the wire
   */
  private[thrift] def handleError(e: Throwable): Unit =
    errorReceiver.counter(Throwables.mkString(e): _*).incr()

  /**
   * Report metrics from Scribe's response.
   */
  private[thrift] def respond(result: Try[ResultCode]): Unit = result match {
    case Return(ResultCode.Ok) => okCounter.incr()
    case Return(ResultCode.TryLater) => tryLaterCounter.incr()
    case Return(e: ResultCode.EnumUnknownResultCode) =>
      errorReceiver.counter(e.name).incr()
    case Throw(e) =>
      errorReceiver.counter(Throwables.mkString(e): _*).incr()

  }
}
