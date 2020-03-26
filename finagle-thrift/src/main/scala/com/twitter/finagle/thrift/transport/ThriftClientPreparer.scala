package com.twitter.finagle.thrift.transport

import com.twitter.finagle.filter.PayloadSizeFilter
import com.twitter.finagle._
import com.twitter.finagle.thrift._
import com.twitter.finagle.thrift.thrift.ConnectionOptions
import com.twitter.util.{Future, Stopwatch}
import org.apache.thrift.protocol.{TBinaryProtocol, TMessage, TMessageType, TProtocolFactory}
import org.apache.thrift.transport.TMemoryInputTransport

/**
 * A class to prepare a client. It adds a payload size filter and a
 * connection validation filter. If requested, it will also prepare
 * clients for upgrade: it attempts to send a probe message to upgrade
 * the protocol to TTwitter. If this succeeds, the TTwitter filter is
 * added.
 */
private[finagle] case class ThriftClientPreparer(
  protocolFactory: TProtocolFactory,
  serviceName: String = "unknown",
  clientId: Option[ClientId] = None,
  useCallerSeqIds: Boolean = false) {

  private def prepareService(
    params: Stack.Params
  )(
    service: Service[ThriftClientRequest, Array[Byte]]
  ): Future[Service[ThriftClientRequest, Array[Byte]]] = {
    val payloadSize = new PayloadSizeFilter[ThriftClientRequest, Array[Byte]](
      params[param.Stats].statsReceiver,
      PayloadSizeFilter.ClientReqTraceKey,
      PayloadSizeFilter.ClientRepTraceKey,
      _.message.length,
      _.length
    )
    val Thrift.param.AttemptTTwitterUpgrade(attemptUpgrade) =
      params[Thrift.param.AttemptTTwitterUpgrade]
    val payloadSizeService = payloadSize.andThen(service)
    val upgradedService =
      if (attemptUpgrade) {
        upgrade(payloadSizeService)
      } else {
        Future.value(payloadSizeService)
      }

    upgradedService.map { upgraded => new ValidateThriftService(upgraded, protocolFactory) }
  }

  def prepare(
    underlying: ServiceFactory[ThriftClientRequest, Array[Byte]],
    params: Stack.Params
  ): ServiceFactory[ThriftClientRequest, Array[Byte]] = {
    val param.Stats(stats) = params[param.Stats]
    val Thrift.param.AttemptTTwitterUpgrade(attemptUpgrade) =
      params[Thrift.param.AttemptTTwitterUpgrade]
    val preparingFactory = underlying.flatMap(prepareService(params))

    if (attemptUpgrade) {
      new ServiceFactoryProxy(preparingFactory) {
        val stat = stats.stat("codec_connection_preparation_latency_ms")
        override def apply(conn: ClientConnection) = {
          val elapsed = Stopwatch.start()
          super.apply(conn).ensure {
            stat.add(elapsed().inMilliseconds)
          }
        }
      }
    } else {
      preparingFactory
    }
  }

  private def upgrade(
    service: Service[ThriftClientRequest, Array[Byte]]
  ): Future[Service[ThriftClientRequest, Array[Byte]]] = {
    // Attempt to upgrade the protocol the first time around by
    // sending a magic method invocation.
    val buffer = new OutputBuffer(protocolFactory)
    buffer().writeMessageBegin(new TMessage(ThriftTracing.CanTraceMethodName, TMessageType.CALL, 0))

    val options = new ConnectionOptions
    options.write(buffer())

    buffer().writeMessageEnd()

    service(new ThriftClientRequest(buffer.toArray, false)).map { bytes =>
      val memoryTransport = new TMemoryInputTransport(bytes)
      val iprot = protocolFactory.getProtocol(memoryTransport)
      val reply = iprot.readMessageBegin()

      val ttwitter = new TTwitterClientFilter(
        serviceName,
        reply.`type` != TMessageType.EXCEPTION,
        clientId,
        protocolFactory
      )
      // TODO: also apply this for Protocols.binaryFactory

      val seqIdFilter =
        if (protocolFactory.isInstanceOf[TBinaryProtocol.Factory] && !useCallerSeqIds)
          new SeqIdFilter
        else
          Filter.identity[ThriftClientRequest, Array[Byte]]

      seqIdFilter.andThen(ttwitter).andThen(service)
    }
  }
}
