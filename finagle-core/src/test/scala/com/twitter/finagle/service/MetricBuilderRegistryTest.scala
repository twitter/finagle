package com.twitter.finagle.service

import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.stats.Percentage
import com.twitter.finagle.stats.exp.Expression
import com.twitter.finagle.stats.exp.ExpressionSchema
import com.twitter.finagle.stats.exp.GreaterThan
import com.twitter.finagle.stats.exp.MonotoneThresholds
import org.scalatest.OneInstancePerTest
import org.scalatest.funsuite.AnyFunSuite

class MetricBuilderRegistryTest extends AnyFunSuite with OneInstancePerTest {

  private class OneCounterBuilder extends MetricBuilderRegistry {
    object Counter extends MetricName

    defineBuilder(Set(Counter)) { results =>
      val expression1 = Expression(results(Counter))

      ExpressionSchema("counterMultiply", expression1.multiply(Expression(100)))
        .withBounds(MonotoneThresholds(GreaterThan, 99.5, 99.97))
        .withUnit(Percentage)
        .withDescription("Counter expression")
    }
  }

  private class TwoCounterBuilder extends MetricBuilderRegistry {
    object Counter1 extends MetricName
    object Counter2 extends MetricName

    defineBuilder(Set(Counter1, Counter2)) { results =>
      val expression1 = Expression(results(Counter1))
      val expression2 = Expression(results(Counter2))

      ExpressionSchema("counterMultiply", expression1.multiply(expression2))
        .withBounds(MonotoneThresholds(GreaterThan, 99.5, 99.97))
        .withUnit(Percentage)
        .withDescription("Counter expression")
    }
  }

  private val sr = new InMemoryStatsReceiver

  test("supports expressions with single arguments") {
    val testBuilder = new OneCounterBuilder

    val counter = sr.counter("count1").metadata.toMetricBuilder.get

    // Nothing yet.
    assert(sr.expressions.size == 0)

    // Set the expression
    testBuilder.setMetricBuilder(testBuilder.Counter, counter, sr)
    assert(sr.expressions.size == 1)
  }

  test("supports compound expressions") {
    val testBuilder = new TwoCounterBuilder

    val counter1 = sr.counter("count1").metadata.toMetricBuilder.get
    val counter2 = sr.counter("count2").metadata.toMetricBuilder.get

    // Nothing yet.
    assert(sr.expressions.size == 0)

    // Set the expression
    testBuilder.setMetricBuilder(testBuilder.Counter1, counter1, sr)
    testBuilder.setMetricBuilder(testBuilder.Counter2, counter2, sr)
    assert(sr.expressions.size == 1)
  }

  test("expressions are not re-registered") {
    val testBuilder = new TwoCounterBuilder

    val counter1 = sr.counter("count1").metadata.toMetricBuilder.get
    val counter2 = sr.counter("count2").metadata.toMetricBuilder.get

    // Nothing yet.
    assert(sr.expressions.size == 0)

    // Expression is not complete with only one counter
    testBuilder.setMetricBuilder(testBuilder.Counter1, counter1, sr)
    assert(sr.expressions.size == 0)

    // Now we complete the expression
    testBuilder.setMetricBuilder(testBuilder.Counter2, counter2, sr)
    assert(sr.expressions.size == 1)

    sr.expressions.clear()

    // We don't attempt to re-register the expression since we've already done it.
    testBuilder.setMetricBuilder(testBuilder.Counter1, counter1, sr)
    assert(sr.expressions.size == 0)
    testBuilder.setMetricBuilder(testBuilder.Counter2, counter2, sr)
    assert(sr.expressions.size == 0)
  }
}
