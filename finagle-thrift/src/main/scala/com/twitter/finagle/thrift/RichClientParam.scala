package com.twitter.finagle.thrift

import com.twitter.finagle.Thrift
import com.twitter.finagle.service.ResponseClassifier
import com.twitter.finagle.stats.{ClientStatsReceiver, StatsReceiver}
import java.util.logging.{Level, Logger}
import org.apache.thrift.protocol.{TBinaryProtocol, TCompactProtocol, TProtocolFactory}

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

  import Protocols._

  /**
   * Apply system-wide read limit on TBinaryProtocol and TCompactProtocol if
   * the System.Property("-Dorg.apache.thrift.readLength") is set.
   */
  val restrictedProtocolFactory: TProtocolFactory = {
    // alter the TProtocol.Factory if system property of readLength is set
    if (SysPropReadLength > NoReadLimit) {
      protocolFactory match {
        case tbf: TBinaryProtocol.Factory => restrictedTBinaryProtocolFactory(tbf)
        case tcf: TCompactProtocol.Factory => restrictedTCompactProtocolFactory(tcf)
        case x => x
      }
    } else protocolFactory
  }

  /**
   * Modify the user defined TBinaryProtocol.Factory if system property of readLength is set
   *
   * We keep other fields as user defined by doing reflection, alter the `stringLengthLimit_`
   * if the system property readLength is more restricted
   */
  @deprecated("Use TBinaryProtocol.Factory(readLengthLimit, NoReadLimit) to set readLengthLimit", "2018-03-09")
  private def restrictedTBinaryProtocolFactory(tbf: TBinaryProtocol.Factory): TBinaryProtocol.Factory = {
    try {
      val strictReadField = tbf.getClass.getDeclaredField("strictRead_")
      strictReadField.setAccessible(true)
      val strictRead = strictReadField.get(tbf).asInstanceOf[Boolean]

      val strictWriteField = tbf.getClass.getDeclaredField("strictWrite_")
      strictWriteField.setAccessible(true)
      val strictWrite = strictWriteField.get(tbf).asInstanceOf[Boolean]

      val stringLengthLimitField = tbf.getClass.getDeclaredField("stringLengthLimit_")
      stringLengthLimitField.setAccessible(true)
      val stringLengthLimit = getReadLimit(stringLengthLimitField.get(tbf).asInstanceOf[Long])

      val containerLengthLimitField = tbf.getClass.getDeclaredField("containerLengthLimit_")
      containerLengthLimitField.setAccessible(true)
      val containerLengthLimit = containerLengthLimitField.get(tbf).asInstanceOf[Long]

      new TBinaryProtocol.Factory(strictRead, strictWrite, stringLengthLimit, containerLengthLimit)
    } catch {
      case _: Throwable => {
        Logger
          .getLogger("finagle-thrift")
          .log(Level.WARNING, "System Property ReadLengthLimit is not applied on ProtocolFactory")
        tbf
      }
    }
  }

  /**
   * Modify the user defined TCompactProtocol.Factory if system property of readLength is set
   *
   * We keep other fields as user defined by doing reflection, alter the `stringLengthLimit_`
   * if the system property readLength is more restricted
   */
  @deprecated("Use TBinaryProtocol.Factory(readLengthLimit, NoReadLimit) to set readLengthLimit", "2018-03-09")
  private def restrictedTCompactProtocolFactory(tcf: TCompactProtocol.Factory): TCompactProtocol.Factory = {
    try {
      val stringLengthLimitField = tcf.getClass.getDeclaredField("stringLengthLimit_")
      stringLengthLimitField.setAccessible(true)
      val stringLengthLimit = getReadLimit(stringLengthLimitField.get(tcf).asInstanceOf[Long])

      val containerLengthLimitField = tcf.getClass.getDeclaredField("containerLengthLimit_")
      containerLengthLimitField.setAccessible(true)
      val containerLengthLimit = containerLengthLimitField.get(tcf).asInstanceOf[Long]

      new TCompactProtocol.Factory(stringLengthLimit, containerLengthLimit)
    } catch {
      case _: Throwable => {
        Logger
          .getLogger("finagle-thrift")
          .log(Level.WARNING, "System Property ReadLengthLimit is not applied on ProtocolFactory")
        tcf
      }
    }
  }
}
