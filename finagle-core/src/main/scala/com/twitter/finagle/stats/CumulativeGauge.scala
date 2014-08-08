package com.twitter.finagle.stats

import scala.ref.WeakReference
import scala.collection.mutable.HashMap

/**
 * CumulativeGauge provides a gauge that is composed of the (addition)
 * of several underlying gauges. It follows the weak reference
 * semantics of Gauges as outlined in StatsReceiver.
 */
private[finagle] trait CumulativeGauge {
  private[this] case class UnderlyingGauge(f: () => Float) extends Gauge {
    def remove() { removeGauge(this) }
  }

  @volatile private[this] var underlying: List[WeakReference[UnderlyingGauge]] = Nil

  /**
   * Returns a buffered version of the current gauges
   */
  private[this] def get(): Seq[UnderlyingGauge] = {
    removeGauge(null)  // GC.
    underlying map { _.get } flatten
  }

  private[this] def removeGauge(underlyingGauge: UnderlyingGauge) = synchronized {
    // This does a GC also.
    underlying = underlying filter { _.get map { _ ne underlyingGauge } getOrElse false }
    if (underlying.isEmpty)
      deregister()
  }

  def addGauge(f: => Float): Gauge = synchronized {
    val shouldRegister = underlying.isEmpty
    val underlyingGauge = UnderlyingGauge(() => f)
    underlying ::= new WeakReference(underlyingGauge)
    if (shouldRegister)
      register()
    underlyingGauge
  }

  def getValue = get() map { _.f() } sum

  /**
   * These need to be implemented by the gauge provider. They indicate
   * when the gauge needs to be registered & deregistered.
   *
   * Special care must be taken in implementing these so that they are free
   * of race conditions.
   */
  def register(): Unit
  def deregister(): Unit
}

trait StatsReceiverWithCumulativeGauges extends StatsReceiver {
  private[this] val gaugeMap = new HashMap[Seq[String], CumulativeGauge]

  /**
   * The StatsReceiver implements these. They provide the cumulated
   * gauges.
   */
  protected[this] def registerGauge(name: Seq[String], f: => Float)
  protected[this] def deregisterGauge(name: Seq[String])

  def addGauge(name: String*)(f: => Float) = synchronized {
    val cumulativeGauge = gaugeMap getOrElseUpdate(name, {
      new CumulativeGauge {
        def register()   = StatsReceiverWithCumulativeGauges.this.registerGauge(name, getValue)
        def deregister() = StatsReceiverWithCumulativeGauges.this synchronized {
          gaugeMap.remove(name)
          StatsReceiverWithCumulativeGauges.this.deregisterGauge(name)
        }
      }
    })

    cumulativeGauge.addGauge(f)
  }
}
