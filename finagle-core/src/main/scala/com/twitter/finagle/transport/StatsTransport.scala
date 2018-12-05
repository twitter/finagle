package com.twitter.finagle.transport

import com.twitter.finagle.stats.{ExceptionStatsHandler, StatsReceiver}
import com.twitter.util.{Future, Throw, Try}

/**
 * A [[TransportProxy]] that collects stats on read/write operations for `underlying`.
 */
class StatsTransport[In, Out](
  underlying: Transport[In, Out],
  exceptionRecorder: ExceptionStatsHandler,
  statsReceiver: StatsReceiver)
    extends TransportProxy(underlying) {
  private[this] val writeScoped = statsReceiver.scope("write")
  private[this] val readScoped = statsReceiver.scope("read")

  private[this] val writeRecordFn: Try[Unit] => Unit = {
    case Throw(exc) => exceptionRecorder.record(writeScoped, exc)
    case _ =>
  }

  private[this] val readRecordFn: Try[Out] => Unit = {
    case Throw(exc) => exceptionRecorder.record(readScoped, exc)
    case _ =>
  }

  def write(in: In): Future[Unit] =
    underlying.write(in).respond(writeRecordFn)

  def read(): Future[Out] =
    underlying.read().respond(readRecordFn)
}
