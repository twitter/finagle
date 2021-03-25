package com.twitter.finagle.thrift

import com.twitter.finagle.Thrift
import com.twitter.finagle.service.ResponseClassifier
import com.twitter.finagle.stats.{ClientStatsReceiver, LazyStatsReceiver, StatsReceiver}
import com.twitter.finagle.thrift.Protocols.{
  NoLimit,
  SysPropContainerLengthLimit,
  SysPropStringLengthLimit,
  limitToOption,
  minLimit
}
import com.twitter.scrooge.TReusableBuffer
import java.util.logging.{Level, Logger}
import org.apache.thrift.protocol.{TBinaryProtocol, TCompactProtocol, TProtocolFactory}

object RichClientParam {
  private[finagle] val NO_THRIFT_REUSABLE_BUFFER_FACTORY: () => TReusableBuffer =
    createThriftReusableBuffer()

  /**
   * Constructs a RichClientParam from the supply parameters.
   *
   * @param protocolFactory A `TProtocolFactory` creates protocol objects from transports
   * @param serviceName For client stats, (default: empty string)
   * @param maxThriftBufferSize The max size of a reusable buffer for the thrift response
   * @param thriftReusableBufferFactory A reusable buffer for the thrift request/response, if passed
   *                                  maxThriftBufferSize is ignored
   * @param responseClassifier See [[com.twitter.finagle.service.ResponseClassifier]]
   * @param clientStats StatsReceiver for recording metrics
   * @param perEndpointStats Whether to record per-endpoint stats, (default: false).
   *                         By enabling this, the specific Thrift Exceptions can be recorded.
   * See [[https://twitter.github.io/finagle/guide/Metrics.html#perendpoint-statsfilter PerEndpoint StatsFilter]]
   */
  def apply(
    protocolFactory: TProtocolFactory = Thrift.param.protocolFactory,
    serviceName: String = "",
    @deprecated("Use thriftReusableBufferFactory instead", "2019-10-03") maxThriftBufferSize: Int =
      Thrift.param.maxThriftBufferSize,
    thriftReusableBufferFactory: () => TReusableBuffer =
      RichClientParam.NO_THRIFT_REUSABLE_BUFFER_FACTORY,
    responseClassifier: ResponseClassifier = ResponseClassifier.Default,
    clientStats: StatsReceiver = ClientStatsReceiver,
    perEndpointStats: Boolean = false
  ): RichClientParam =
    new RichClientParam(
      protocolFactory,
      serviceName,
      maxThriftBufferSize,
      thriftReusableBufferFactory,
      responseClassifier,
      new LazyStatsReceiver(clientStats),
      perEndpointStats
    )

  def apply(
    protocolFactory: TProtocolFactory,
    serviceName: String,
    maxThriftBufferSize: Int,
    responseClassifier: ResponseClassifier,
    clientStats: StatsReceiver
  ): RichClientParam =
    apply(
      protocolFactory = protocolFactory,
      serviceName = serviceName,
      maxThriftBufferSize = maxThriftBufferSize,
      responseClassifier = responseClassifier,
      clientStats = clientStats,
      perEndpointStats = false
    )

  def apply(
    protocolFactory: TProtocolFactory,
    maxThriftBufferSize: Int,
    responseClassifier: ResponseClassifier
  ): RichClientParam =
    apply(protocolFactory, "", maxThriftBufferSize, responseClassifier, ClientStatsReceiver)

  def apply(
    protocolFactory: TProtocolFactory,
    responseClassifier: ResponseClassifier
  ): RichClientParam =
    apply(protocolFactory, Thrift.param.maxThriftBufferSize, responseClassifier)

  def apply(protocolFactory: TProtocolFactory): RichClientParam =
    apply(protocolFactory, ResponseClassifier.Default)

  def apply(): RichClientParam = apply(Thrift.param.protocolFactory)

  /**
   * Creates a new TReusableBuffer. The function can be used to create a partially applied
   * function which in turn can be passed as parameter thriftReusableBufferFactory to
   * RichClientParam ctor
   * @param thriftBufferSize The max size of a reusable buffer for the thrift response
   * @return TReusableBuffer
   */
  def createThriftReusableBuffer(
    thriftBufferSize: Int = Thrift.param.maxThriftBufferSize
  )(
  ): TReusableBuffer = {
    TReusableBuffer(initialSize = thriftBufferSize, maxThriftBufferSize = thriftBufferSize)
  }

  /**
   * Apply system-wide read limit on TBinaryProtocol and TCompactProtocol if
   * any of the following System Properties are set:
   *   org.apache.thrift.readLength (deprecated)
   *   com.twitter.finagle.thrift.stringLengthLimit
   *   com.twitter.finagle.thrift.containerLengthLimit
   */
  private[thrift] def restrictedProtocolFactory(
    protocolFactory: TProtocolFactory
  ): TProtocolFactory = {
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

/**
 * Thrift-specific parameters for configuring clients.
 */
case class RichClientParam private (
  val protocolFactory: TProtocolFactory,
  val serviceName: String,
  @deprecated("Use thriftReusableBufferFactory instead", "2019-10-03") val maxThriftBufferSize: Int,
  val thriftReusableBufferFactory: () => TReusableBuffer,
  val responseClassifier: ResponseClassifier,
  val clientStats: StatsReceiver,
  val perEndpointStats: Boolean) {

  @deprecated("Use RichClientParam#apply instead", "2019-11-19")
  def this(protocolFactory: TProtocolFactory, responseClassifier: ResponseClassifier) = this(
    protocolFactory,
    "",
    Thrift.param.maxThriftBufferSize,
    RichClientParam.NO_THRIFT_REUSABLE_BUFFER_FACTORY,
    responseClassifier,
    new LazyStatsReceiver(ClientStatsReceiver),
    false
  )

  @deprecated("Use RichClientParam#apply instead", "2019-11-19")
  def this(protocolFactory: TProtocolFactory) = this(protocolFactory, ResponseClassifier.Default)

  @deprecated("Use RichClientParam#apply instead", "2019-11-19")
  def this() = this(Thrift.param.protocolFactory, ResponseClassifier.Default)

  def copy(
    protocolFactory: TProtocolFactory = this.protocolFactory,
    serviceName: String = this.serviceName,
    @deprecated("Use thriftReusableBufferFactory instead", "2019-10-03") maxThriftBufferSize: Int =
      this.maxThriftBufferSize,
    thriftReusableBufferFactory: () => TReusableBuffer = this.thriftReusableBufferFactory,
    responseClassifier: ResponseClassifier = this.responseClassifier,
    clientStats: StatsReceiver = this.clientStats,
    perEndpointStats: Boolean = this.perEndpointStats
  ): RichClientParam =
    new RichClientParam(
      protocolFactory,
      serviceName,
      maxThriftBufferSize,
      thriftReusableBufferFactory,
      responseClassifier,
      clientStats,
      perEndpointStats
    )

  def createThriftReusableBuffer(): TReusableBuffer = {
    if (thriftReusableBufferFactory eq RichClientParam.NO_THRIFT_REUSABLE_BUFFER_FACTORY) {
      RichClientParam.createThriftReusableBuffer(maxThriftBufferSize)()
    } else {
      thriftReusableBufferFactory()
    }
  }

  /**
   * Apply system-wide read limit on TBinaryProtocol and TCompactProtocol if
   * any of the following System Properties are set:
   *   org.apache.thrift.readLength (deprecated)
   *   com.twitter.finagle.thrift.stringLengthLimit
   *   com.twitter.finagle.thrift.containerLengthLimit
   */
  val restrictedProtocolFactory: TProtocolFactory =
    RichClientParam.restrictedProtocolFactory(protocolFactory)
}
