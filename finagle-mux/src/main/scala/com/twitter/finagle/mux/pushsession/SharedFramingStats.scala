package com.twitter.finagle.mux.pushsession

import com.twitter.finagle.stats.{StatsReceiver, Verbosity}
import java.util.concurrent.atomic.LongAdder

/**
 * Keeping separate stats for each connection could be expensive for servers operating under a high
 * concurrency (tens of thousands connections).
 *
 * Instead we register them in a single place and share across multiple connections. A very similar
 * approach is taken in [[com.twitter.finagle.netty4.channel.SharedChannelStats]].
 *
 * Explicit resource clean up for the underlying gauges isn't required. As there are usually very
 * few instances of this class (one per server and one per client), it's sufficient to release
 * gauges via a weak reference (when the instance is collected).
 */
private[mux] final class SharedFramingStats(sr: StatsReceiver, verbosity: Verbosity) {

  private[this] val writeStreamsGauge = sr.addGauge(verbosity, "pending_write_streams") {
    pendingWriteStreams.floatValue()
  }

  private[this] val readStreamsGauge = sr.addGauge(verbosity, "pending_read_streams") {
    pendingReadStreams.floatValue()
  }

  val writeStreamBytes = sr.stat(verbosity, "write_stream_bytes")
  val readStreamBytes = sr.stat(verbosity, "read_stream_bytes")

  val pendingWriteStreams = new LongAdder()
  val pendingReadStreams = new LongAdder()
}
