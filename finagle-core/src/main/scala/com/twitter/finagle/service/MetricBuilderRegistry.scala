package com.twitter.finagle.service

import com.twitter.finagle.stats.MetricBuilder
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.stats.exp.ExpressionSchema
import com.twitter.logging.Logger
import scala.util.control.NonFatal

/**
 * Helper class useful for defining expressions.
 *
 * Derivatives of this class can be used as a way to register relevant metrics
 * and have the associated expressions automatically registered.
 */
private[twitter] abstract class MetricBuilderRegistry {

  private[this] val log = Logger()

  private[this] val builders = scala.collection.mutable.Map
    .empty[Set[MetricName], Map[MetricName, MetricBuilder] => ExpressionSchema]

  private[this] val registeredMetrics =
    scala.collection.mutable.Map.empty[MetricName, (MetricBuilder, StatsReceiver)]

  abstract class MetricName

  def setMetricBuilder(
    metricName: MetricName,
    metricBuilder: MetricBuilder,
    sr: StatsReceiver
  ): Unit = synchronized {
    registeredMetrics.update(metricName, metricBuilder -> sr)
    evaluateEntries()
  }

  protected def defineBuilder(
    requirements: Set[MetricName]
  )(
    f: Map[MetricName, MetricBuilder] => ExpressionSchema
  ): Unit = synchronized {
    require(requirements.nonEmpty)
    builders += requirements -> f
    evaluateEntries()
  }

  // Check entries and see if any of them are fully defined, and if so, register them.
  private[this] def evaluateEntries(): Unit = {
    val toRemove = scala.collection.mutable.ArrayBuffer.empty[Set[MetricName]]
    builders.foreach {
      case (requirements, f) =>
        if (requirements.forall(registeredMetrics.contains(_))) {
          // Good news! We have all we need.
          toRemove += requirements
          try buildExpression(requirements, f)
          catch {
            case NonFatal(t) =>
              log.error(t, s"Failed to build expression with $requirements requirements.")
          }
        }
    }

    toRemove.foreach(builders.remove(_))
  }

  private[this] def buildExpression(
    requirements: Set[MetricName],
    f: Map[MetricName, MetricBuilder] => ExpressionSchema
  ): Unit = {
    // See if we have all the metrics already
    val values = requirements.map { metricName =>
      val (metadata, sr) = registeredMetrics(metricName)
      (metricName, metadata, sr)
    }

    val (_, _, sr) = values.head
    val expr = f(values.iterator.map { case (name, metadata, _) => name -> metadata }.toMap)
    sr.registerExpression(expr)
  }
}
