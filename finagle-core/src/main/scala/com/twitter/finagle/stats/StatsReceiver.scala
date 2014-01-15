package com.twitter.finagle.stats

import com.twitter.util.{Future, Stopwatch, JavaSingleton}
import java.util.concurrent.TimeUnit

/**
 * A writeable Counter. Only sums are kept of Counters. An example
 * Counter is "number of requests served".
 */
trait Counter {
  def incr(delta: Int)
  def incr() { incr(1) }
}

/**
 * An append-only collection of time-series data. Example Stats are
 * "queue depth" or "query width in a stream of requests".
 */
trait Stat {
  def add(value: Float)
}

/**
 * Exposes the value of a function. For example, one could add a gauge for a
 * computed health metric.
 */
trait Gauge {
  def remove()
}

object StatsReceiver {
  private[StatsReceiver] var immortalGauges: List[Gauge] = Nil
}

trait StatsReceiver {
  /**
   * Specifies the representative receiver.  This is in order to
   * expose an object we can use for comparison so that global stats
   * are only reported once per receiver.
   */
  val repr: AnyRef

  /**
   * Accurately indicates if this is a NullStatsReceiver.
   * Because equality is not forwarded via scala.Proxy, this
   * is helpful to check for a NullStatsReceiver.
   */
  def isNull: Boolean = false

  /**
   * Time a given function using the given TimeUnit
   */
  def time[T](unit: TimeUnit, stat: Stat)(f: => T): T = {
    val elapsed = Stopwatch.start()
    val result = f
    stat.add(elapsed().inUnit(unit))
    result
  }

  /**
   * Time a given function using the given TimeUnit
   */
  def time[T](unit: TimeUnit, name: String*)(f: => T): T = {
    time(unit, stat(name: _*))(f)
  }

  /**
   * Time a given function in milliseconds
   */
  def time[T](name: String*)(f: => T): T = {
    time(TimeUnit.MILLISECONDS, name: _*)(f)
  }

  /**
   * Time a given future using the given TimeUnit
   */
  def timeFuture[T](unit: TimeUnit, stat: Stat)(f: => Future[T]): Future[T] = {
    val elapsed = Stopwatch.start()
    f ensure {
      stat.add(elapsed().inUnit(unit))
    }
  }

  /**
   * Time a given future using the given TimeUnit
   */
  def timeFuture[T](unit: TimeUnit, name: String*)(f: => Future[T]): Future[T] = {
    timeFuture(unit, stat(name: _*))(f)
  }

  /**
   * Time a given future in milliseconds
   */
  def timeFuture[T](name: String*)(f: => Future[T]): Future[T] = {
    timeFuture(TimeUnit.MILLISECONDS, name: _*)(f)
  }

  /**
   * Get a Counter with the description
   */
  def counter(name: String*): Counter

  /**
   * Get a Counter with the description. This method is a convenience for Java program.
   */
  def counter0(name: String): Counter = counter(name)

  /**
   * Get a Stat with the description
   */
  def stat(name: String*): Stat

  /**
   * Get a Stat with the description. This method is a convenience for Java programs.
   */
  def stat0(name: String): Stat = stat(name)

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
  def scope(namespace: String): StatsReceiver = {
    val seqPrefix = Seq(namespace)
    new NameTranslatingStatsReceiver(this) {
      protected[this] def translate(name: Seq[String]) = seqPrefix ++ name
    }
  }

  /**
   * Prepend a suffix value to the next scope
   * stats.scopeSuffix("toto").scope("client").counter("adds") will generate
   * /client/toto/adds
   */
  def scopeSuffix(suffix: String): StatsReceiver = {
    val self = this
    new StatsReceiver {
      val repr = self.repr

      def counter(names: String*) = self.counter(names: _*)
      def stat(names: String*)    = self.stat(names: _*)
      def addGauge(names: String*)(f: => Float) = self.addGauge(names: _*)(f)

      override def scope(namespace: String) = self.scope(namespace).scope(suffix)
    }
  }
}

trait StatsReceiverProxy extends StatsReceiver {
  def self: StatsReceiver

  val repr = self
  override def isNull = self.isNull
  def counter(names: String*) = self.counter(names:_*)
  def stat(names: String*) = self.stat(names:_*)
  def addGauge(names: String*)(f: => Float) = self.addGauge(names:_*)(f)
}

abstract class NameTranslatingStatsReceiver(val self: StatsReceiver)
  extends StatsReceiver
{
  protected[this] def translate(name: Seq[String]): Seq[String]
  val repr = self.repr
  override def isNull = self.isNull

  def counter(name: String*) = self.counter(translate(name): _*)
  def stat(name: String*)    = self.stat(translate(name): _*)
  def addGauge(name: String*)(f: => Float) = self.addGauge(translate(name): _*)(f)
}

class NullStatsReceiver extends StatsReceiver with JavaSingleton {
  val repr = this
  override def isNull = true

  private[this] val NullCounter = new Counter { def incr(delta: Int) {} }
  private[this] val NullStat = new Stat { def add(value: Float) {}}
  private[this] val NullGauge = new Gauge { def remove() {} }

  def counter(name: String*) = NullCounter
  def stat(name: String*) = NullStat
  def addGauge(name: String*)(f: => Float) = NullGauge
}

object NullStatsReceiver extends NullStatsReceiver

object DefaultStatsReceiver extends {
  val self: StatsReceiver = LoadedStatsReceiver
} with StatsReceiverProxy

object ClientStatsReceiver extends {
  val self: StatsReceiver = LoadedStatsReceiver.scope("clnt")
} with StatsReceiverProxy

object ServerStatsReceiver extends {
  val self: StatsReceiver = LoadedStatsReceiver.scope("srv")
} with StatsReceiverProxy
