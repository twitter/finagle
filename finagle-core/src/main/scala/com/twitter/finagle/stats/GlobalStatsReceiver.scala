package com.twitter.finagle.stats

import collection.mutable
import ref.WeakReference

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
