package com.twitter.finagle.util

import com.twitter.ostrich.StatsProvider

class OstrichSampleRepository(prefix: String, suffix: String, statsProvider: StatsProvider)
  extends ObservableSampleRepository
{
  def tails[A](s: Seq[A]): Seq[Seq[A]] = {
    s match {
      case s@Seq(_) =>
        Seq(s)

      case Seq(hd, tl@_*) =>
        Seq(Seq(hd)) ++ (tails(tl) map { t => Seq(hd) ++ t })
    }
  }

  def observeAdd(path: Seq[String], value: Int, count: Int) {
    // TODO: count vs. value.
    tails(path) foreach { path =>
      statsProvider.addTiming(prefix + (path mkString "__"), count)
    }

    statsProvider.addTiming(prefix + (path mkString "__") + "_" + suffix, count)
  }
}

