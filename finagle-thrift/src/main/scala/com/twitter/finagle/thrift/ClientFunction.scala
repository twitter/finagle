package com.twitter.finagle.thrift

import com.twitter.finagle.Service
import com.twitter.finagle.SourcedException
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.service.ReqRep
import com.twitter.finagle.service.ResponseClass
import com.twitter.finagle.service.ResponseClassifier
import com.twitter.finagle.stats.Counter
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.thrift.ClientDeserializeCtx.Key
import com.twitter.finagle.thrift.Headers.Request
import com.twitter.scrooge.TReusableBuffer
import com.twitter.scrooge.ThriftStruct
import com.twitter.util.Future
import com.twitter.util.Throwables
import com.twitter.util.Try
import org.apache.thrift.protocol.TMessage
import org.apache.thrift.protocol.TMessageType
import org.apache.thrift.protocol.TProtocolFactory

object ClientFunction {

  /**
   * Serialize a given request {@code inputArgs} and send the serialized
   * request over the wire to a finagle service {@code service}, and
   * classify the deserialized response Upon receiving it from the server.
   *
   * @param clientFuncNameForWire the over the wire function name
   * @param replyDeserializer the deserializer used to create a
   *                          [[ClientDeserializeCtx]]
   * @param inputArgs the request to serialize
   * @param serviceName used to exception metrics scoping
   * @param service the service to execute the serialized request, and
   *                return a deserialized response
   * @param responseClassifier classify the deserialized response
   * @param tlReusableBuffer a buffer to call [[encodeRequest]]
   * @param protocolFactory a [[TProtocolFactory]] to call
   *                        [[encodeRequest]]
   * @param failuresScope a counter scope to report exceptions
   * @param requestCounter a counter to report number of request
   * @param successCounter a counter to report number of successful
   *                       request
   * @param failuresCounter a counter to report number of failed
   *                        request
   * @tparam T The type of deserialized response
   *
   * @note This method is intended to be invoked from Scrooge generated
   *       finagle client, typical scrooge users should not need to call
   *       it directly.
   */
  def serde[T](
    clientFuncNameForWire: String,
    replyDeserializer: Array[Byte] => Try[T],
    inputArgs: ThriftStruct,
    serviceName: String,
    service: Service[ThriftClientRequest, Array[Byte]],
    responseClassifier: PartialFunction[ReqRep, ResponseClass],
    tlReusableBuffer: TReusableBuffer,
    protocolFactory: TProtocolFactory,
    failuresScope: StatsReceiver,
    requestCounter: Counter,
    successCounter: Counter,
    failuresCounter: Counter
  ): Future[T] = {
    requestCounter.incr()

    val serdeCtx = new ClientDeserializeCtx[T](inputArgs, replyDeserializer)
    Contexts.local.let(
      Key,
      serdeCtx,
      Request.Key,
      Request.newValues
    ) {
      serdeCtx.rpcName(clientFuncNameForWire)
      val start = System.nanoTime
      val serialized =
        encodeRequest(clientFuncNameForWire, inputArgs, tlReusableBuffer, protocolFactory)
      serdeCtx.serializationTime(System.nanoTime - start)
      service(serialized)
        .flatMap { response =>
          Future.const(serdeCtx.deserialize(response))
        }.respond { response =>
          val classified =
            responseClassifier.applyOrElse(ReqRep(inputArgs, response), ResponseClassifier.Default)
          classified match {
            case _: ResponseClass.Successful =>
              successCounter.incr()
            case _: ResponseClass.Failed =>
              failuresCounter.incr()
              if (response.isThrow) {
                SourcedException.setServiceName(response.throwable, serviceName)
                failuresScope.counter(Throwables.mkString(response.throwable): _*).incr()
              }
            case _ =>
          } // Last ResponseClass is Ignorable, which we do not need to record
        }
    }
  }

  private[this] def encodeRequest(
    name: String,
    args: ThriftStruct,
    tlReusableBuffer: TReusableBuffer,
    protocolFactory: TProtocolFactory
  ): ThriftClientRequest = {
    val memoryBuffer = tlReusableBuffer.take()
    try {
      val oprot = protocolFactory.getProtocol(memoryBuffer)

      oprot.writeMessageBegin(new TMessage(name, TMessageType.CALL, 0))
      args.write(oprot)
      oprot.writeMessageEnd()
      oprot.getTransport.flush()
      val bytes = java.util.Arrays.copyOfRange(
        memoryBuffer.getArray(),
        0,
        memoryBuffer.length()
      )
      new ThriftClientRequest(bytes, false)
    } finally {
      tlReusableBuffer.reset()
    }
  }

}
