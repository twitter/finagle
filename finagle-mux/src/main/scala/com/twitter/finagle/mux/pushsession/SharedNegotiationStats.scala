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
private[finagle] final class SharedNegotiationStats(
  sr: StatsReceiver,
  framerVerbosity: Verbosity = Verbosity.Debug,
  tlsVerbosity: Verbosity = Verbosity.Default) {

  val pendingWriteStreams = new LongAdder()
  val pendingReadStreams = new LongAdder()

  val writeStreamBytes = sr.stat(framerVerbosity, "mux", "framer", "write_stream_bytes")
  val readStreamBytes = sr.stat(framerVerbosity, "mux", "framer", "read_stream_bytes")

  val tlsSuccess = sr.counter(tlsVerbosity, "mux", "tls", "upgrade", "success")
  val tlsFailures = sr.counter(tlsVerbosity, "mux", "tls", "upgrade", "incompatible")

  val compressionSuccess = sr.counter(tlsVerbosity, "mux", "compression", "upgrade", "success")
  val compressionFailures =
    sr.counter(tlsVerbosity, "mux", "compression", "upgrade", "incompatible")

  val decompressionSuccess = sr.counter(tlsVerbosity, "mux", "decompression", "upgrade", "success")
  val decompressionFailures =
    sr.counter(tlsVerbosity, "mux", "decompression", "upgrade", "incompatible")

  private[this] val writeStreamsGauge =
    sr.addGauge(framerVerbosity, "mux", "framer", "pending_write_streams") {
      pendingWriteStreams.floatValue()
    }

  private[this] val readStreamsGauge =
    sr.addGauge(framerVerbosity, "mux", "framer", "pending_read_streams") {
      pendingReadStreams.floatValue()
    }
}
