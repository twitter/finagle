package com.twitter.finagle.thrift.partitioning

import com.twitter.finagle.Stack
import com.twitter.finagle.Thrift.param
import com.twitter.finagle.thrift.{RichClientParam, ThriftClientRequest}
import com.twitter.scrooge.{TReusableBuffer, ThriftStruct}
import java.util
import org.apache.thrift.protocol.{TMessage, TMessageType, TProtocolFactory}

/**
 * Used by ThriftPartitioningService for message fan-out.
 * @param params Stack.Params to provide protocolFactory and TReusableBuffer.
 *               Use default ones if not set.
 */
private[partitioning] class ThriftRequestSerializer(params: Stack.Params) {

  private[this] val protocolFactory: TProtocolFactory =
    RichClientParam.restrictedProtocolFactory(params[param.ProtocolFactory].protocolFactory)

  private[this] val thriftReusableBuffer: TReusableBuffer =
    params[param.TReusableBufferFactory].tReusableBufferFactory()

  /**
   * Serialize a Thrift object request to bytes, this object request is split from the original
   * fan-out request.
   *
   * @param methodName Thrift method name
   * @param args       Thrift object request
   * @param oneWay     Expect response or not, this should inherit from the original request
   */
  def serialize(methodName: String, args: ThriftStruct, oneWay: Boolean): ThriftClientRequest = {
    val memoryBuffer = thriftReusableBuffer.get()
    try {
      val oprot = protocolFactory.getProtocol(memoryBuffer)
      oprot.writeMessageBegin(new TMessage(methodName, TMessageType.CALL, 0))
      args.write(oprot)
      oprot.writeMessageEnd()
      oprot.getTransport().flush()
      val bytes = util.Arrays.copyOfRange(memoryBuffer.getArray(), 0, memoryBuffer.length())
      new ThriftClientRequest(bytes, oneWay)
    } finally thriftReusableBuffer.reset()
  }
}
