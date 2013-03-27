package com.twitter.finagle.stats

/**
 * A RollupStatsReceiver reports stats on multiple Counter/Stat/Gauge based on the sequence of
 * names you pass.
 * e.g.
 * counter("errors", "clientErrors", "java_net_ConnectException").incr()
 * will actually increment those three counters:
 * - "/errors"
 * - "/errors/clientErrors"
 * - "/errors/clientErrors/java_net_ConnectException"
 */
class RollupStatsReceiver(val self: StatsReceiver)
  extends StatsReceiver with Proxy
{
  val repr = self.repr

  private[this] def tails[A](s: Seq[A]): Seq[Seq[A]] = {
    s match {
      case s@Seq(_) =>
        Seq(s)

      case Seq(hd, tl@_*) =>
        Seq(Seq(hd)) ++ (tails(tl) map { t => Seq(hd) ++ t })
    }
  }

  def counter(names: String*) = new Counter {
    private[this] val allCounters = BroadcastCounter(
      tails(names) map (self.counter(_: _*))
    )
    def incr(delta: Int) = allCounters.incr(delta)
  }

  def stat(names: String*) = new Stat {
    private[this] val allStats = BroadcastStat(
      tails(names) map (self.stat(_: _*))
    )
    def add(value: Float) = allStats.add(value)
  }

  def addGauge(names: String*)(f: => Float) = new Gauge {
    private[this] val underlying = tails(names) map { self.addGauge(_: _*)(f) }
    def remove() = underlying foreach { _.remove() }
  }
}
