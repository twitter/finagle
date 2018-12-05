package com.twitter.finagle.thrift

import com.twitter.finagle.filter.PayloadSizeFilter
import com.twitter.finagle.{ServiceFactory, Stack, param}
import org.apache.thrift.protocol.TProtocolFactory

private[finagle] case class ThriftServerPreparer(
  protocolFactory: TProtocolFactory,
  serviceName: String) {
  private[this] val uncaughtExceptionsFilter =
    new UncaughtAppExceptionFilter(protocolFactory)

  def prepare(
    factory: ServiceFactory[Array[Byte], Array[Byte]],
    params: Stack.Params
  ): ServiceFactory[Array[Byte], Array[Byte]] = factory.map { service =>
    val payloadSize = new PayloadSizeFilter[Array[Byte], Array[Byte]](
      params[param.Stats].statsReceiver,
      PayloadSizeFilter.ServerReqTraceKey,
      PayloadSizeFilter.ServerRepTraceKey,
      _.length,
      _.length
    )

    val ttwitter = new TTwitterServerFilter(serviceName, protocolFactory)

    payloadSize.andThen(ttwitter).andThen(uncaughtExceptionsFilter).andThen(service)
  }
}
