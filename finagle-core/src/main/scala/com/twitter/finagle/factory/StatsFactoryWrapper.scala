package com.twitter.finagle.factory

import com.twitter.finagle._
import com.twitter.util.Throwables
import com.twitter.finagle.stats.{StatsReceiver, RollupStatsReceiver}
import com.twitter.util.{Future, Stopwatch, Return, Throw}

private[finagle] object StatsFactoryWrapper {
  val role = Stack.Role("ServiceCreationStats")

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.factory.StatsFactoryWrapper]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module1[param.Stats, ServiceFactory[Req, Rep]] {
      val role = StatsFactoryWrapper.role
      val description = "Track statistics on service creation failures " +
        "and service acquisition latency"
      def make(_stats: param.Stats, next: ServiceFactory[Req, Rep]) = {
        val param.Stats(statsReceiver) = _stats
        if (statsReceiver.isNull) next
        else
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
class StatsFactoryWrapper[Req, Rep](self: ServiceFactory[Req, Rep], statsReceiver: StatsReceiver)
    extends ServiceFactoryProxy[Req, Rep](self) {
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
