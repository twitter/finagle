package com.twitter.finagle.stats

/**
 * A writeable Counter. Only sums are kept of Counters.  An example
 * Counter is "number of requests served".
 */
trait Counter extends {
  def incr(delta: Int)
  def incr() { incr(1) }
}

/** 
 * TODO: doc 
 */ 
trait Stat {
  def add(value: Float) { add(value, 1) }
  def add(value: Float, count: Int)
}

trait StatsReceiver {
  /**
   * Get a Counter with the description
   */
  def counter(name: String*): Counter

  /**
   * Get a Gauge with the description
   */
  def stat(name: String*): Stat

  /**
   * Register a function to be periodically measured.
   */
  def provideGauge(name: String*)(f: => Float)

  /**
   * Prepend ``namespace'' to the names of this receiver.
   */
  def scope(namespace: String) = {
    val seqPrefix = Seq(namespace)
    new NameTranslatingStatsReceiver(this) {
      protected[this] def translate(name: Seq[String]) = seqPrefix ++ name
    }
  }

  def withSuffix(namespace: String) = {
    val seqSuffix = Seq(namespace)
    new NameTranslatingStatsReceiver(this) {
      protected[this] def translate(name: Seq[String]) = name ++ seqSuffix
    }
  }
}

class RollupStatsReceiver(val self: StatsReceiver)
  extends StatsReceiver with Proxy
{
  private[this] def tails[A](s: Seq[A]): Seq[Seq[A]] = {
    s match {
      case s@Seq(_) =>
        Seq(s)

      case Seq(hd, tl@_*) =>
        Seq(Seq(hd)) ++ (tails(tl) map { t => Seq(hd) ++ t })
    }
  }

  def counter(name: String*) = new Counter {
    private[this] val allCounters = tails(name) map (self.counter(_: _*))
    def incr(delta: Int) = allCounters foreach (_.incr(delta))
  }

  def stat(name: String*) = new Stat {
    private[this] val allStats = tails(name) map (self.stat(_: _*))
    def add(value: Float, count: Int) = allStats foreach (_.add(value, count))
  }

  def provideGauge(name: String*)(f: => Float) =
    tails(name) foreach { self.provideGauge(_: _*)(f) }
}

abstract class NameTranslatingStatsReceiver(val self: StatsReceiver)
  extends StatsReceiver with Proxy
{
  protected[this] def translate(name: Seq[String]): Seq[String]

  def counter(name: String*)                   = self.counter(translate(name): _*)
  def stat(name: String*)                      = self.stat(translate(name): _*)
  def provideGauge(name: String*)(f: => Float) = self.provideGauge(translate(name): _*)(f)
}

object NullStatsReceiver extends StatsReceiver {
  def counter(name: String*) = new Counter {
    def incr(delta: Int) {}
  }

  def stat(name: String*) = new Stat {
    def add(value: Float, count: Int) {}
  }

  def provideGauge(name: String*)(f: => Float) {}
}
