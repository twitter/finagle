package com.twitter.finagle.stats

import com.twitter.finagle.service.ResponseClassifier

/**
 * Utilities to instrument Standard Service Metrics in [[StatFilter]]s.
 * By default it is [[Disabled]].
 */
private[finagle] sealed trait StandardStats

private[finagle] case object Disabled extends StandardStats

/**
 * Injects a [[StandardStatsReceiver]] which creates a set of standard metrics besides
 * the existing metrics, use [[ResponseClassifier.Default]] to calculate rpc metrics
 *
 * @param standardStatsReceiver see [[StandardStatsReceiver]]
 */
private[finagle] case class StatsOnly(standardStatsReceiver: StandardStatsReceiver)
    extends StandardStats

/**
 * Injects a [[StandardStatsReceiver]] and a [[ResponseClassifier]] to create a new set of
 * standard metrics besides the existing metrics
 *
 * @param standardStatsReceiver see [[StandardStatsReceiver]]
 * @param responseClassifier for measuring standard rpc metrics, for example, use
 *                           [[ThriftResponseClassifier.ThriftExceptionsAsFailures]]
 *                           to calculate thrift success and failures
 */
private[finagle] case class StatsAndClassifier(
  standardStatsReceiver: StandardStatsReceiver,
  responseClassifier: ResponseClassifier)
    extends StandardStats
