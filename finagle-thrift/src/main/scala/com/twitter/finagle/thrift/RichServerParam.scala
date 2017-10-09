package com.twitter.finagle.thrift

import com.twitter.finagle.Thrift
import com.twitter.finagle.service.ResponseClassifier
import com.twitter.finagle.stats.{LoadedStatsReceiver, StatsReceiver}
import org.apache.thrift.protocol.TProtocolFactory

/**
 * Produce a server with params wrapped in RichServerParam
 *
 * @param protocolFactory A `TProtocolFactory` creates protocol objects from transports
 * @param serviceName For server stats, (default: "thrift")
 * @param maxThriftBufferSize The max size of a reusable buffer for the thrift response
 * @param serverStats StatsReceiver for recording metrics
 */
case class RichServerParam(
  protocolFactory: TProtocolFactory = Thrift.param.protocolFactory,
  serviceName: String = "thrift",
  maxThriftBufferSize: Int = Thrift.param.maxThriftBufferSize,
  serverStats: StatsReceiver = LoadedStatsReceiver,
  responseClassifier: ResponseClassifier = ResponseClassifier.Default
) {

  def this(
    protocolFactory: TProtocolFactory,
    maxThriftBufferSize: Int
  ) = this(protocolFactory, "thrift", maxThriftBufferSize, LoadedStatsReceiver, ResponseClassifier.Default)

  def this(
    protocolFactory: TProtocolFactory
  ) = this(protocolFactory, Thrift.param.maxThriftBufferSize)

  def this() = this(Thrift.param.protocolFactory)
}
