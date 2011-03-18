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
  def add(value: Float)
}

trait Gauge {
  def remove()
}

object StatsReceiver {
  private[StatsReceiver] var immortalGauges: List[Gauge] = Nil
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
   * Register a function to be periodically measured. This measurement
   * exists in perpetuity. Measurements under the same name are added
   * together.
   */
  def provideGauge(name: String*)(f: => Float) {
    val gauge = addGauge(name: _*)(f)
    StatsReceiver.synchronized {
      StatsReceiver.immortalGauges ::= gauge
    }
  }

  /**
   * Add the function ``f'' as a gauge with the given name. The
   * returned gauge value is only weakly referenced by the
   * StatsReceiver, and if garbage collected will cease to be a part
   * of this measurement: thus, it needs to be retained by the
   * caller. Immortal measurements are made with ``provideGauge''. As
   * with ``provideGauge'', gauges with equal names are added
   * together.
   */
  def addGauge(name: String*)(f: => Float): Gauge

  /**
   * Prepend ``namespace'' to the names of this receiver.
   */
  def scope(namespace: String) = {
    val seqPrefix = Seq(namespace)
    new NameTranslatingStatsReceiver(this) {
      protected[this] def translate(name: Seq[String]) = seqPrefix ++ name
    }
  }

  /**
   * Append ``namespace'' to the names of this receiver.
   */
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
    def add(value: Float) = allStats foreach (_.add(value))
  }

  def addGauge(name: String*)(f: => Float) = new Gauge {
    private[this] val underlying = tails(name) map { self.addGauge(_: _*)(f) }
    def remove() = underlying foreach { _.remove() }
  }
}

abstract class NameTranslatingStatsReceiver(val self: StatsReceiver)
  extends StatsReceiver with Proxy
{
  protected[this] def translate(name: Seq[String]): Seq[String]

  def counter(name: String*) = self.counter(translate(name): _*)
  def stat(name: String*)    = self.stat(translate(name): _*)

  def addGauge(name: String*)(f: => Float) = self.addGauge(translate(name): _*)(f)
}

object NullStatsReceiver extends StatsReceiver {
  def counter(name: String*) = new Counter {
    def incr(delta: Int) {}
  }

  def stat(name: String*) = new Stat {
    def add(value: Float) {}
  }

  def addGauge(name: String*)(f: => Float) = new Gauge { def remove() {} }
}
