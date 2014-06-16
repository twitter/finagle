package com.twitter.finagle.factory

import com.twitter.finagle._
import com.twitter.finagle.util.Throwables
import com.twitter.finagle.stats.{StatsReceiver, RollupStatsReceiver}
import com.twitter.util.{Future, Stopwatch, Return, Throw}

private[finagle] object StatsFactoryWrapper {
  object ServiceCreationStats extends Stack.Role

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.StatsFactoryWrapper]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Simple[ServiceFactory[Req, Rep]](ServiceCreationStats) {
      val description = "Track statistics on service creation failures " +
        "and service acquisition latency"
      def make(params: Params, next: ServiceFactory[Req, Rep]) = {
        val param.Stats(statsReceiver) = params[param.Stats]
        new StatsFactoryWrapper(
          next,
          new RollupStatsReceiver(statsReceiver.scope("service_creation"))
        )
      }
    }
}

/**
 * A [[com.twitter.finagle.ServiceFactoryProxy]] that tracks statistics on
 * [[com.twitter.finagle.Service]] creation failures and service acquisition latency.
 */
class StatsFactoryWrapper[Req, Rep](
    self: ServiceFactory[Req, Rep],
    statsReceiver: StatsReceiver)
  extends ServiceFactoryProxy[Req, Rep](self)
{
  private[this] val failureStats = statsReceiver.scope("failures")
  private[this] val latencyStat = statsReceiver.stat("service_acquisition_latency_ms")

  override def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
    val elapsed = Stopwatch.start()
    super.apply(conn) respond {
      case Throw(t) =>
        failureStats.counter(Throwables.mkString(t): _*).incr()
        latencyStat.add(elapsed().inMilliseconds)
      case Return(_) =>
        latencyStat.add(elapsed().inMilliseconds)
    }
  }
}
