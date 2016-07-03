package com.twitter.finagle.transport

import com.twitter.finagle.stats.{ExceptionStatsHandler, StatsReceiver}
import com.twitter.util.Future

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

    private[this] val writeRecordFn: Throwable => Unit = { exc =>
      exceptionRecorder.record(writeScoped, exc)
    }

    private[this] val readRecordFn: Throwable => Unit = { exc =>
      exceptionRecorder.record(readScoped, exc)
    }

    def write(in: In): Future[Unit] =
      underlying.write(in).onFailure(writeRecordFn)

    def read(): Future[Out] =
      underlying.read().onFailure(readRecordFn)
  }