package com.twitter.finagle.thrift.service

import com.twitter.finagle.context.Contexts
import com.twitter.finagle.thrift.ClientDeserializeCtx
import com.twitter.finagle.thrift.ThriftClientRequest
import com.twitter.finagle.thrift.maxReusableBufferSize
import com.twitter.finagle.Filter
import com.twitter.finagle.Service
import com.twitter.finagle.SourcedException
import com.twitter.scrooge.TReusableBuffer
import com.twitter.scrooge.ThriftMethod
import com.twitter.scrooge.ThriftStruct
import com.twitter.scrooge.ThriftStructCodec
import com.twitter.util.Future
import com.twitter.util.Return
import com.twitter.util.Throw
import com.twitter.util.Try
import java.util.Arrays
import org.apache.thrift.TApplicationException
import org.apache.thrift.protocol.TMessage
import org.apache.thrift.protocol.TMessageType
import org.apache.thrift.protocol.TProtocolFactory
import org.apache.thrift.transport.TMemoryInputTransport

object ThriftCodec {

  /**
   * A [[Filter]] that wraps a binary thrift Service[ThriftClientRequest, Array[Byte]]
   * and produces a [[Service]] from a [[ThriftStruct]] to [[ThriftClientRequest]] (i.e. bytes).
   */
  private[thrift] def filter(
    method: ThriftMethod,
    pf: TProtocolFactory
  ): Filter[method.Args, method.SuccessType, ThriftClientRequest, Array[Byte]] =
    new Filter[method.Args, method.SuccessType, ThriftClientRequest, Array[Byte]] {
      private[this] val decodeRepFn: Array[Byte] => Try[method.SuccessType] = { bytes =>
        decodeResponse(bytes, method.responseCodec, pf).flatMap { result: method.Result =>
          result.firstException() match {
            case Some(ex) => Throw(ex)
            case None =>
              result.successField match {
                case Some(v) => Return(v)
                case None =>
                  Throw(
                    new TApplicationException(
                      TApplicationException.MISSING_RESULT,
                      s"Thrift method '${method.name}' failed: missing result"
                    )
                  )
              }
          }
        }
      }

      def apply(
        args: method.Args,
        service: Service[ThriftClientRequest, Array[Byte]]
      ): Future[method.SuccessType] = {
        val request = encodeRequest(method.name, args, pf, method.oneway)
        val serdeCtx = new ClientDeserializeCtx[method.SuccessType](args, decodeRepFn)
        serdeCtx.rpcName(method.name)
        Contexts.local.let(ClientDeserializeCtx.Key, serdeCtx) {
          service(request).flatMap { response => Future.const(serdeCtx.deserialize(response)) }
        }
      }
    }

  private[this] val tlReusableBuffer = TReusableBuffer(
    maxThriftBufferSize = maxReusableBufferSize().inBytes.toInt
  )

  private def encodeRequest(
    methodName: String,
    args: ThriftStruct,
    pf: TProtocolFactory,
    oneway: Boolean
  ): ThriftClientRequest = {
    val buf = tlReusableBuffer.take()
    val oprot = pf.getProtocol(buf)

    oprot.writeMessageBegin(new TMessage(methodName, TMessageType.CALL, 0))
    args.write(oprot)
    oprot.writeMessageEnd()

    val bytes = Arrays.copyOfRange(buf.getArray(), 0, buf.length())
    tlReusableBuffer.reset()

    new ThriftClientRequest(bytes, oneway)
  }

  def decodeResponse[T <: ThriftStruct](
    resBytes: Array[Byte],
    codec: ThriftStructCodec[T],
    pf: TProtocolFactory,
    serviceName: String = ""
  ): Try[T] = {
    val iprot = pf.getProtocol(new TMemoryInputTransport(resBytes))
    val msg = iprot.readMessageBegin()
    if (msg.`type` == TMessageType.EXCEPTION) {
      val exception = TApplicationException.readFrom(iprot)
      iprot.readMessageEnd()
      Throw(SourcedException.setServiceName(exception, serviceName))
    } else {
      val result = codec.decode(iprot)
      iprot.readMessageEnd()
      Return(result)
    }
  }
}
