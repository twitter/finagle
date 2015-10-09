package com.twitter.finagle.failters

import com.twitter.util.{Witness, Var}
import com.twitter.finagle.stats.StatsReceiver

object Failter {
  val DefaultSeed = 42L
}

trait Failter {
  def probability: Var[Double]
  def seed: Long
  def stats: StatsReceiver

  @volatile
  protected var prob: Double = _
  probability.changes.register(Witness({ newProb => prob = newProb }))

  protected val probGauge = stats.provideGauge("probability") { prob.toFloat }
  protected val rejectedStat = stats.counter("rejected")
  protected val passedStat = stats.counter("passed")

  protected val rand = new scala.util.Random(seed)
}
