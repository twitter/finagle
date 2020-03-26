package com.twitter.finagle.netty4.channel

import com.twitter.finagle.Stack
import com.twitter.finagle.param.Stats
import com.twitter.finagle.stats.{StatsReceiver, Verbosity}
import io.netty.channel.SingleThreadEventLoop
import java.util.concurrent.atomic.LongAdder

/**
 * Stores all stats that are aggregated across all channels for the client
 * or server.
 */
private[finagle] class SharedChannelStats(params: Stack.Params) {
  protected val statsReceiver: StatsReceiver = params[Stats].statsReceiver

  protected val connectionCount = new LongAdder()
  def connectionCountIncrement(): Unit = connectionCount.increment()
  def connectionCountDecrement(): Unit = connectionCount.decrement()

  protected val tlsConnectionCount = new LongAdder()
  def tlsConnectionCountIncrement(): Unit = tlsConnectionCount.increment()
  def tlsConnectionCountDecrement(): Unit = tlsConnectionCount.decrement()

  @volatile private var eventLoops: Set[SingleThreadEventLoop] = Set.empty
  def registerEventLoop(e: SingleThreadEventLoop): Unit =
    synchronized { eventLoops = eventLoops + e }
  def unregisterEventLoop(e: SingleThreadEventLoop): Unit =
    synchronized { eventLoops = eventLoops - e }

  val connects = statsReceiver.counter("connects")

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

  private val connections = statsReceiver.addGauge("connections") {
    connectionCount.sum()
  }
  private val tlsConnections = statsReceiver.addGauge("tls", "connections") {
    tlsConnectionCount.sum()
  }
  private val pendingIoEvents = statsReceiver.addGauge("pending_io_events") {
    eventLoops.foldLeft(0.0f)((acc, el) => acc + el.pendingTasks())
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
