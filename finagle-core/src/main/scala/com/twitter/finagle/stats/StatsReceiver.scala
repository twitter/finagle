package com.twitter.finagle.stats

import scala.collection.mutable
import scala.ref.WeakReference

import com.twitter.util.{Future, Stopwatch, JavaSingleton}
import java.util.concurrent.TimeUnit

/**
 * A writeable Counter. Only sums are kept of Counters. An example
 * Counter is "number of requests served".
 */
trait Counter extends {
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
   * Append ``namespace'' to the names of this receiver.
   */
  def withSuffix(namespace: String): StatsReceiver = {
    val seqSuffix = Seq(namespace)
    new NameTranslatingStatsReceiver(this) {
      protected[this] def translate(name: Seq[String]) = name ++ seqSuffix
    }
  }
}

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
  val repr = self.repr

  def counter(name: String*) = self.counter(translate(name): _*)
  def stat(name: String*)    = self.stat(translate(name): _*)

  def addGauge(name: String*)(f: => Float) = self.addGauge(translate(name): _*)(f)
}

class NullStatsReceiver extends StatsReceiver with JavaSingleton {
  val repr = this

  private[this] val NullCounter = new Counter { def incr(delta: Int) {} }
  private[this] val NullStat = new Stat { def add(value: Float) {}}
  private[this] val NullGauge = new Gauge { def remove() {} }

  def counter(name: String*) = NullCounter
  def stat(name: String*) = NullStat
  def addGauge(name: String*)(f: => Float) = NullGauge
}

object NullStatsReceiver extends NullStatsReceiver

object DefaultStatsReceiver extends NullStatsReceiver

/** In-memory stats receiver for testing. */
class InMemoryStatsReceiver extends StatsReceiver {
  val repr = this

  val counters = new mutable.HashMap[Seq[String], Int]
                   with mutable.SynchronizedMap[Seq[String], Int]
  val stats    = new mutable.HashMap[Seq[String], Seq[Float]]
                   with mutable.SynchronizedMap[Seq[String], Seq[Float]]
  val gauges   = new mutable.WeakHashMap[Seq[String], () => Float]
                   with mutable.SynchronizedMap[Seq[String], () => Float]

  def counter(name: String*): Counter = {
    new Counter {
      def incr(delta: Int) {
        val oldValue = counters.get(name).getOrElse(0)
        counters(name) = oldValue + delta
      }
    }
  }

  def stat(name: String*): Stat = {
    new Stat {
      def add(value: Float) {
        val oldValue = stats.get(name).getOrElse(Seq.empty)
        stats(name) = oldValue :+ value
      }
    }
  }

  def addGauge(name: String*)(f: => Float): Gauge = {
    val gauge = new Gauge {
      def remove() {
        gauges -= name
      }
    }
    gauges += name -> (() => f)
    gauge
  }
}

/**
 * Note: currently supports only gauges, will throw
 * away other types.
 */
class GlobalStatsReceiver extends NullStatsReceiver {
  private[this] trait GlobalGauge extends Gauge { def addReceiver(receiver: StatsReceiver) }
  private[this] val registered = new mutable.HashMap[AnyRef, StatsReceiver]
  private[this] val gauges = new mutable.HashMap[Seq[String], WeakReference[GlobalGauge]]

  private[this] def mkGauge(name: Seq[String], f: => Float) = new GlobalGauge {
    private[this] var children: List[Gauge] = Nil

    gauges(name) = new WeakReference(this)
    // Add onto current receivers.
    registered.values foreach { addReceiver(_) }

    def addReceiver(receiver: StatsReceiver) = {
      children ::= receiver.addGauge(name: _*) { f }
    }

    def remove() = GlobalStatsReceiver.this.synchronized {
      gauges.remove(name)
      children foreach { _.remove() }
      children = Nil
    }
  }

  def register(receiver: StatsReceiver): Unit = synchronized {
    if (receiver eq this) return

    val refs = if (registered contains receiver.repr) Seq() else {
      registered += receiver.repr -> receiver
      gauges.values.toBuffer
    }

    for (ref <- refs; gauge <- ref.get)
      gauge.addReceiver(receiver)
  }

  override val repr = this

  override def addGauge(name: String*)(f: => Float): Gauge = synchronized {
    val gauge0 = for {
      ref <- gauges.get(name)
      gauge <- ref.get
    } yield gauge

    gauge0 getOrElse mkGauge(name, f)
  }
}

object GlobalStatsReceiver extends GlobalStatsReceiver
