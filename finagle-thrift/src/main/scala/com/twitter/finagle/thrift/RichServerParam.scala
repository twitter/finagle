package com.twitter.finagle.thrift

import com.twitter.finagle.Thrift
import com.twitter.finagle.service.ResponseClassifier
import com.twitter.finagle.stats.{LoadedStatsReceiver, StatsReceiver}
import java.util.logging.{Level, Logger}
import org.apache.thrift.protocol.{TBinaryProtocol, TCompactProtocol, TProtocolFactory}

/**
 * Produce a server with params wrapped in RichServerParam
 *
 * @param protocolFactory A `TProtocolFactory` creates protocol objects from transports
 * @param serviceName For server stats, (default: "thrift")
 * @param maxThriftBufferSize The max size of a reusable buffer for the thrift response
 * @param serverStats StatsReceiver for recording metrics
 * @param responseClassifier See [[com.twitter.finagle.service.ResponseClassifier]]
 * @param perEndpointStats Whether to record per-endpoint stats, (default: false).
 *                         By enabling this, the specific Thrift Exceptions can be recorded.
 * See [[https://twitter.github.io/finagle/guide/Metrics.html#perendpoint-statsfilter PerEndpoint StatsFilter]]
 */
case class RichServerParam(
  protocolFactory: TProtocolFactory = Thrift.param.protocolFactory,
  serviceName: String = "thrift",
  maxThriftBufferSize: Int = Thrift.param.maxThriftBufferSize,
  serverStats: StatsReceiver = LoadedStatsReceiver,
  responseClassifier: ResponseClassifier = ResponseClassifier.Default,
  perEndpointStats: Boolean = false) {

  def this(
    protocolFactory: TProtocolFactory,
    serviceName: String,
    maxThriftBufferSize: Int,
    serverStats: StatsReceiver,
    responseClassifier: ResponseClassifier
  ) =
    this(protocolFactory, serviceName, maxThriftBufferSize, serverStats, responseClassifier, false)

  def this(protocolFactory: TProtocolFactory, maxThriftBufferSize: Int) =
    this(
      protocolFactory,
      "thrift",
      maxThriftBufferSize,
      LoadedStatsReceiver,
      ResponseClassifier.Default
    )

  def this(protocolFactory: TProtocolFactory) =
    this(protocolFactory, Thrift.param.maxThriftBufferSize)

  def this() = this(Thrift.param.protocolFactory)

  import Protocols._

  /**
   * Apply system-wide read limit on TBinaryProtocol and TCompactProtocol if
   * any of the following System Properties are set:
   *   org.apache.thrift.readLength (deprecated)
   *   com.twitter.finagle.thrift.stringLengthLimit
   *   com.twitter.finagle.thrift.containerLengthLimit
   */
  val restrictedProtocolFactory: TProtocolFactory = {
    // alter the TProtocol.Factory if system property of readLength is set
    if (SysPropStringLengthLimit.isDefined || SysPropContainerLengthLimit.isDefined) {
      protocolFactory match {
        case tbf: TBinaryProtocol.Factory => restrictedTBinaryProtocolFactory(tbf)
        case tcf: TCompactProtocol.Factory => restrictedTCompactProtocolFactory(tcf)
        case x => x
      }
    } else protocolFactory
  }

  /**
   * Modify the user defined TBinaryProtocol.Factory if system properties of `stringLengthLimit`
   * and/or `containerLengthLimit` are set.
   *
   * We keep other fields as user defined by doing reflection, alter the `stringLengthLimit_`
   * and/or `containerLengthLimit_` if the system property limits are more restricted.
   */
  @deprecated(
    "Use TBinaryProtocol.Factory($stringLengthLimit, $containerLengthLimit) to set " +
      "stringLengthLimit and containerLengthLimit",
    "2018-03-09")
  private def restrictedTBinaryProtocolFactory(
    tbf: TBinaryProtocol.Factory
  ): TBinaryProtocol.Factory = {
    try {
      val strictReadField = tbf.getClass.getDeclaredField("strictRead_")
      strictReadField.setAccessible(true)
      val strictRead = strictReadField.get(tbf).asInstanceOf[Boolean]

      val strictWriteField = tbf.getClass.getDeclaredField("strictWrite_")
      strictWriteField.setAccessible(true)
      val strictWrite = strictWriteField.get(tbf).asInstanceOf[Boolean]

      val stringLengthLimitField = tbf.getClass.getDeclaredField("stringLengthLimit_")
      stringLengthLimitField.setAccessible(true)
      val stringLengthLimit = minLimit(
        limitToOption(stringLengthLimitField.get(tbf).asInstanceOf[Long]),
        SysPropStringLengthLimit).getOrElse(NoLimit)

      val containerLengthLimitField = tbf.getClass.getDeclaredField("containerLengthLimit_")
      containerLengthLimitField.setAccessible(true)
      val containerLengthLimit = minLimit(
        limitToOption(containerLengthLimitField.get(tbf).asInstanceOf[Long]),
        SysPropContainerLengthLimit).getOrElse(NoLimit)

      new TBinaryProtocol.Factory(strictRead, strictWrite, stringLengthLimit, containerLengthLimit)
    } catch {
      case _: Throwable => {
        Logger
          .getLogger("finagle-thrift")
          .log(Level.WARNING, "System Property length limits are not applied on ProtocolFactory")
        tbf
      }
    }
  }

  /**
   * Modify the user defined TCompactProtocol.Factory if system properties of `stringLengthLimit`
   * and/or `containerLengthLimit` are set.
   *
   * We keep other fields as user defined by doing reflection, alter the `stringLengthLimit_`
   * and/or `containerLengthLimit_` if the system property limits are more restricted.
   */
  @deprecated(
    "Use TCompactProtocol.Factory($stringLengthLimit, $containerLengthLimit) to set " +
      "stringLengthLimit and containerLengthLimit",
    "2018-03-09")
  private def restrictedTCompactProtocolFactory(
    tcf: TCompactProtocol.Factory
  ): TCompactProtocol.Factory = {
    try {
      val stringLengthLimitField = tcf.getClass.getDeclaredField("stringLengthLimit_")
      stringLengthLimitField.setAccessible(true)
      val stringLengthLimit = minLimit(
        limitToOption(stringLengthLimitField.get(tcf).asInstanceOf[Long]),
        SysPropStringLengthLimit).getOrElse(NoLimit)

      val containerLengthLimitField = tcf.getClass.getDeclaredField("containerLengthLimit_")
      containerLengthLimitField.setAccessible(true)
      val containerLengthLimit = minLimit(
        limitToOption(containerLengthLimitField.get(tcf).asInstanceOf[Long]),
        SysPropContainerLengthLimit).getOrElse(NoLimit)

      new TCompactProtocol.Factory(stringLengthLimit, containerLengthLimit)
    } catch {
      case _: Throwable => {
        Logger
          .getLogger("finagle-thrift")
          .log(Level.WARNING, "System Property length limits are not applied on ProtocolFactory")
        tcf
      }
    }
  }
}
