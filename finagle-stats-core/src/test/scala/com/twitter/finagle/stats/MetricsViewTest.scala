package com.twitter.finagle.stats

import java.util
import java.util.Collections
import scala.collection.JavaConverters._
import org.scalatest.funsuite.AnyFunSuite

class MetricsViewTest extends AnyFunSuite {

  private val EmptySnapshot: Snapshot = new Snapshot {
    def count: Long = 0L
    def sum: Long = 0L
    def max: Long = 0L
    def min: Long = 0L
    def average: Double = 0L
    def percentiles: IndexedSeq[Snapshot.Percentile] = IndexedSeq.empty
  }

  private class Impl(
    val gauges: util.Map[String, Number] = Collections.emptyMap[String, Number],
    val counters: util.Map[String, Number] = Collections.emptyMap[String, Number],
    val histograms: util.Map[String, Snapshot] = Collections.emptyMap[String, Snapshot],
    val verbosity: util.Map[String, Verbosity] = Collections.emptyMap[String, Verbosity])
      extends MetricsView

  private val a1 = new Impl(
    gauges = Collections.singletonMap("a", 1),
    counters = Collections.singletonMap("a", 1),
    histograms = Collections.singletonMap("a", EmptySnapshot),
    verbosity = Collections.singletonMap("a", Verbosity.Debug)
  )

  private val b2 = new Impl(
    gauges = Collections.singletonMap("b", 2),
    counters = Collections.singletonMap("b", 2),
    histograms = Collections.singletonMap("b", EmptySnapshot),
    verbosity = Collections.singletonMap("b", Verbosity.Debug)
  )

  test("of") {
    val aAndB = MetricsView.of(a1, b2)
    assert(Map("a" -> 1, "b" -> 2) == aAndB.gauges.asScala)
    assert(Map("a" -> 1, "b" -> 2) == aAndB.counters.asScala)
    assert(Map("a" -> EmptySnapshot, "b" -> EmptySnapshot) == aAndB.histograms.asScala)
    assert(Map("a" -> Verbosity.Debug, "b" -> Verbosity.Debug) == aAndB.verbosity.asScala)
  }

  test("of handles duplicates") {
    val c = new Impl(
      gauges = Collections.singletonMap("a", 2),
      counters = Collections.singletonMap("a", 2),
      histograms = Collections.singletonMap("a", EmptySnapshot),
      verbosity = Collections.singletonMap("a", Verbosity.Default)
    )
    val aAndC = MetricsView.of(a1, c)
    assert(Map("a" -> 1) == aAndC.gauges.asScala)
    assert(Map("a" -> 1) == aAndC.counters.asScala)
    assert(Map("a" -> EmptySnapshot) == aAndC.histograms.asScala)
    assert(Map("a" -> Verbosity.Debug) == aAndC.verbosity.asScala)

    val cAndA = MetricsView.of(c, a1)
    assert(Map("a" -> 2) == cAndA.gauges.asScala)
    assert(Map("a" -> 2) == cAndA.counters.asScala)
    assert(Map("a" -> EmptySnapshot) == cAndA.histograms.asScala)
    assert(Map("a" -> Verbosity.Default) == cAndA.verbosity.asScala)
  }

  test("of ignores empty maps") {
    val empty = new Impl()
    val aAndEmpty = MetricsView.of(a1, empty)
    assert(Map("a" -> 1) == aAndEmpty.gauges.asScala)
    assert(Map("a" -> 1) == aAndEmpty.counters.asScala)
    assert(Map("a" -> EmptySnapshot) == aAndEmpty.histograms.asScala)
    assert(Map("a" -> Verbosity.Debug) == aAndEmpty.verbosity.asScala)

    val emptyAndA = MetricsView.of(empty, a1)
    assert(Map("a" -> 1) == emptyAndA.gauges.asScala)
    assert(Map("a" -> 1) == emptyAndA.counters.asScala)
    assert(Map("a" -> EmptySnapshot) == emptyAndA.histograms.asScala)
    assert(Map("a" -> Verbosity.Debug) == emptyAndA.verbosity.asScala)
  }

}
