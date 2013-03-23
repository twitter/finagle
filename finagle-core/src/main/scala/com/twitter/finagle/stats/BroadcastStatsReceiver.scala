package com.twitter.finagle.stats

/**
 * BroadcastStatsReceiver is a helper object that create a StatsReceiver wrapper around multiple
 * StatsReceivers (n).
 * For performance reason, we have specialized cases if n == (0, 1 or 2)
 */
object BroadcastStatsReceiver {
  def apply(receivers: Seq[StatsReceiver]) = receivers.filterNot(_ == NullStatsReceiver) match {
    case Seq() => NullStatsReceiver
    case Seq(fst) => fst
    case Seq(fst, snd) => new Two(fst, snd)
    case more => new N(more)
  }

  private class Two(first: StatsReceiver, second: StatsReceiver)
    extends StatsReceiver
  {
    val repr = this

    def counter(names: String*) = new Counter {
      val counter1 = first.counter(names:_*)
      val counter2 = second.counter(names:_*)
      def incr(delta: Int) {
        counter1.incr(delta)
        counter2.incr(delta)
      }
    }

    def stat(names: String*) = new Stat {
      val stat1 = first.stat(names:_*)
      val stat2 = second.stat(names:_*)
      def add(value: Float) {
        stat1.add(value)
        stat2.add(value)
      }
    }

    def addGauge(names: String*)(f: => Float) = new Gauge {
      val gauge1 = first.addGauge(names:_*)(f)
      val gauge2 = second.addGauge(names:_*)(f)
      def remove() {
        gauge1.remove()
        gauge2.remove()
      }
    }

    override def scope(name: String) =
      new Two(first.scope(name), second.scope(name))
  }

  private class N(statsReceivers: Seq[StatsReceiver]) extends StatsReceiver {
    val repr = this

    def counter(names: String*) = new Counter {
      val counters = statsReceivers map { _.counter(names:_*) }
      def incr(delta: Int) {
        counters foreach { _.incr(delta) }
      }
    }
    def stat(names: String*) = new Stat {
      val stats = statsReceivers map { _.stat(names:_*) }
      def add(value: Float) {
        stats foreach { _.add(value) }
      }
    }
    def addGauge(names: String*)(f: => Float) = new Gauge {
      val gauges = statsReceivers map { _.addGauge(names:_*)(f) }
      def remove() = gauges foreach { _.remove() }
    }

    override def scope(name: String) =
      new N(statsReceivers map { _.scope(name) })
  }
}
