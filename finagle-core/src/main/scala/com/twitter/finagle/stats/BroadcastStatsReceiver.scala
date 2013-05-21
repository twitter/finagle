package com.twitter.finagle.stats

/**
 * BroadcastStatsReceiver is a helper object that create a StatsReceiver wrapper around multiple
 * StatsReceivers (n).
 */
object BroadcastStatsReceiver {
  def apply(receivers: Seq[StatsReceiver]) = receivers.filterNot(_.isNull) match {
    case Seq() => NullStatsReceiver
    case Seq(fst) => fst
    case more => new N(more)
  }

  private class N(statsReceivers: Seq[StatsReceiver]) extends StatsReceiver {
    val repr = this

    def counter(names: String*) = new Counter {
      val broadcastCounter = BroadcastCounter(
        statsReceivers map { _.counter(names:_*) }
      )
      def incr(delta: Int) {
        broadcastCounter.incr(delta)
      }
    }
    def stat(names: String*) = new Stat {
      val broadcastStats = BroadcastStat(
        statsReceivers map { _.stat(names:_*) }
      )
      def add(value: Float) {
        broadcastStats.add(value)
      }
    }
    def addGauge(names: String*)(f: => Float) = new Gauge {
      val gauges = statsReceivers map { _.addGauge(names:_*)(f) }
      def remove() = gauges foreach { _.remove() }
    }
  }
}

/**
 * BroadcastCounter is a helper object that create a Counter wrapper around multiple
 * Counters (n).
 * For performance reason, we have specialized cases if n == (0, 1, 2, 3 or 4)
 */
object BroadcastCounter {
  def apply(counters: Seq[Counter]) = counters match {
    case Seq() => NullCounter
    case Seq(counter) => counter
    case Seq(a, b) => new Two(a, b)
    case Seq(a, b, c) => new Three(a, b, c)
    case Seq(a, b, c, d) => new Four(a, b, c, d)
    case more => new N(more)
  }

  private object NullCounter extends Counter {
    def incr(delta: Int) {}
  }

  private class Two(a: Counter, b: Counter) extends Counter {
    def incr(delta: Int) {
      a.incr(delta)
      b.incr(delta)
    }
  }

  private class Three(a: Counter, b: Counter, c: Counter) extends Counter {
    def incr(delta: Int) {
      a.incr(delta)
      b.incr(delta)
      c.incr(delta)
    }
  }

  private class Four(a: Counter, b: Counter, c: Counter, d: Counter) extends Counter {
    def incr(delta: Int) {
      a.incr(delta)
      b.incr(delta)
      c.incr(delta)
      d.incr(delta)
    }
  }

  private class N(counters: Seq[Counter]) extends Counter {
    def incr(delta: Int) { counters.foreach(_.incr(delta)) }
  }
}

/**
 * BroadcastStat is a helper object that create a Counter wrapper around multiple
 * Stats (n).
 * For performance reason, we have specialized cases if n == (0, 1, 2, 3 or 4)
 */
object BroadcastStat {
  def apply(stats: Seq[Stat]) = stats match {
    case Seq() => NullStat
    case Seq(counter) => counter
    case Seq(a, b) => new Two(a, b)
    case Seq(a, b, c) => new Three(a, b, c)
    case Seq(a, b, c, d) => new Four(a, b, c, d)
    case more => new N(more)
  }

  private object NullStat extends Stat {
    def add(value: Float) {}
  }

  private class Two(a: Stat, b: Stat) extends Stat {
    def add(value: Float) {
      a.add(value)
      b.add(value)
    }
  }

  private class Three(a: Stat, b: Stat, c: Stat) extends Stat {
    def add(value: Float) {
      a.add(value)
      b.add(value)
      c.add(value)
    }
  }

  private class Four(a: Stat, b: Stat, c: Stat, d: Stat) extends Stat {
    def add(value: Float) {
      a.add(value)
      b.add(value)
      c.add(value)
      d.add(value)
    }
  }

  private class N(stats: Seq[Stat]) extends Stat {
    def add(value: Float) { stats.foreach(_.add(value)) }
  }
}
