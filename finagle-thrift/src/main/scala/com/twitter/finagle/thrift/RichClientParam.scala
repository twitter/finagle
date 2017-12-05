package com.twitter.finagle.thrift

import com.twitter.finagle.Thrift
import com.twitter.finagle.service.ResponseClassifier
import com.twitter.finagle.stats.{ClientStatsReceiver, StatsReceiver}
import org.apache.thrift.protocol.TProtocolFactory

/**
 * Produce a client with params wrapped in RichClientParam
 *
 * @param protocolFactory A `TProtocolFactory` creates protocol objects from transports
 * @param serviceName For client stats, (default: empty string)
 * @param maxThriftBufferSize The max size of a reusable buffer for the thrift response
 * @param responseClassifier  See [[com.twitter.finagle.service.ResponseClassifier]]
 * @param clientStats StatsReceiver for recording metrics
 */
case class RichClientParam(
  protocolFactory: TProtocolFactory = Thrift.param.protocolFactory,
  serviceName: String = "",
  maxThriftBufferSize: Int = Thrift.param.maxThriftBufferSize,
  responseClassifier: ResponseClassifier = ResponseClassifier.Default,
  clientStats: StatsReceiver = ClientStatsReceiver
) {

  def this(
    protocolFactory: TProtocolFactory,
    maxThriftBufferSize: Int,
    responseClassifier: ResponseClassifier
  ) = this(protocolFactory, "", maxThriftBufferSize, responseClassifier, ClientStatsReceiver)

  def this(
    protocolFactory: TProtocolFactory,
    responseClassifier: ResponseClassifier
  ) = this(protocolFactory, Thrift.param.maxThriftBufferSize, responseClassifier)

  def this(
    protocolFactory: TProtocolFactory
  ) = this(protocolFactory, ResponseClassifier.Default)

  def this() = this(Thrift.param.protocolFactory)
}
