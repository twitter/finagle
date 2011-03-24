package com.twitter.finagle.stats

import ref.WeakReference
import collection.mutable.WeakHashMap


/**
 * CumulativeGauge provides a gauge that is composed of the (addition)
 * of several underlying gauges. It follows the weak reference
 * semantics of Gauges as outlined in StatsReceiver.
 */
private[finagle] trait CumulativeGauge {
  private[this] case class UnderlyingGauge(f: () => Float) extends Gauge {
    def remove() { removeGauge(this) }
  }

  private[this] var underlying: List[WeakReference[UnderlyingGauge]] = Nil

  private[this] def get() = synchronized {
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

  def getValue = synchronized {
    get() map { _.f() } sum
  }

  /**
   * These need to be implemented by the gauge provider. They indicate
   * when the gauge needs to be registered & deregistered.
   */
  def register(): Unit
  def deregister(): Unit
}

trait StatsReceiverWithCumulativeGauges extends StatsReceiver {
  private[this] val gaugeMap = new WeakHashMap[Seq[String], CumulativeGauge]

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
        def deregister() = StatsReceiverWithCumulativeGauges.this.deregisterGauge(name)
      }
    })

    cumulativeGauge.addGauge(f)
  }
}
