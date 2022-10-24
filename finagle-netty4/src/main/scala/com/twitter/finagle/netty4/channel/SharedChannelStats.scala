package com.twitter.finagle.netty4.channel

import com.twitter.finagle.Stack
import com.twitter.finagle.param.Stats
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.stats.Verbosity
import java.util.concurrent.atomic.LongAdder

/**
 * Stores all stats that are aggregated across all channels for the client
 * or server.
 */
private[finagle] class SharedChannelStats(params: Stack.Params) {
  private[this] val statsReceiver: StatsReceiver = params[Stats].statsReceiver

  private[this] val connectionCount = new LongAdder()
  private[this] val tlsConnectionCount = new LongAdder()

  private[this] val connects = statsReceiver.counter("connects")

  /** Called when a channel connection event occurs */
  def connectionIncrement(): Unit = {
    connects.incr()
    connectionCount.increment()
  }

  /** Called when a channel disconnect event occurs */
  def connectionDecrement(): Unit = connectionCount.decrement()

  /** Called when a channel TLS connection event occurs */
  def tlsConnectionIncrement(): Unit = tlsConnectionCount.increment()

  /** Called when a channel TLS disconnect event occurs */
  def tlsConnectionDecrement(): Unit = tlsConnectionCount.decrement()

  val connectionDuration =
    statsReceiver.stat(Verbosity.Debug, "connection_duration")
  val connectionReceivedBytes =
    statsReceiver.stat(Verbosity.Debug, "connection_received_bytes")
  val connectionSentBytes =
    statsReceiver.stat(Verbosity.Debug, "connection_sent_bytes")
  val writable =
    statsReceiver.counter(Verbosity.Debug, "socket_writable_ms")
  val unwritable =
    statsReceiver.counter(Verbosity.Debug, "socket_unwritable_ms")
  val retransmits =
    statsReceiver.counter(Verbosity.Debug, name = "tcp_retransmits")
  val tcpSendWindowSize =
    statsReceiver.stat(Verbosity.Debug, name = "tcp_send_window_size")

  val receivedBytes = statsReceiver.counter("received_bytes")
  val sentBytes = statsReceiver.counter("sent_bytes")
  val exceptions = statsReceiver.scope("exn")
  val closesCount = statsReceiver.counter("closes")

  private[this] val connections = statsReceiver.addGauge("connections") {
    connectionCount.sum()
  }
  private[this] val tlsConnections = statsReceiver.addGauge("tls", "connections") {
    tlsConnectionCount.sum()
  }
}

private[finagle] object SharedChannelStats {

  def apply(params: Stack.Params): SharedChannelStats =
    new SharedChannelStats(params)

  /**
   * An internal-only `Stack` param which allows a developer
   * to control which [[SharedChannelStats]] implementation is
   * constructed and added to the [[ChannelStatsHandler]]. In some
   * cases it may be necessary to extend the [[SharedChannelStats]]
   * class and report metrics that do not make sense for all protocols
   * and scenarios.
   *
   * @param fn A function which takes all `Stack.Params` and constructs
   *           a [[SharedChannelStats]] implementation.
   */
  case class Param(fn: Stack.Params => SharedChannelStats) {
    def mk(): (Param, Stack.Param[Param]) =
      (this, Param.param)
  }
  object Param {
    implicit val param = Stack.Param(Param(SharedChannelStats.apply _))
  }

}
