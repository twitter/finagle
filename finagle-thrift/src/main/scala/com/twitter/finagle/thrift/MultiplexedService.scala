package com.twitter.finagle.thrift

import com.twitter.finagle.{Service, Thrift}
import com.twitter.scrooge.TReusableMemoryTransport
import com.twitter.util.Future
import java.util.Arrays
import org.apache.thrift.TApplicationException
import org.apache.thrift.protocol._
import org.apache.thrift.transport.TMemoryInputTransport
import scala.collection.JavaConverters._
import scala.util.Try

class MultiplexedFinagleService(
  services: Map[String, Service[Array[Byte], Array[Byte]]],
  protocolFactory: TProtocolFactory,
  maxThriftBufferSize: Int = Thrift.Server.maxThriftBufferSize)
    extends Service[Array[Byte], Array[Byte]] {

  private val serviceMap = services.mapValues(getFunctionMap)

  def getFunctionMap(
    service: Service[Array[Byte], Array[Byte]]
  ): collection.Map[String, (TProtocol, Int) => Future[Array[Byte]]] = {
    Try(scroogeFinagleServiceFunctionMap(service))
      .orElse(Try(thriftFinagleServiceFunctionMap(service)))
      .getOrElse(throw new IllegalArgumentException("%s cannot be multiplexed".format(service.getClass.getName)))
  }

  def scroogeFinagleServiceFunctionMap[T](target: AnyRef) = {
    val m = target.getClass.getMethod("functionMap")
    val accessible = m.isAccessible
    m.setAccessible(true)
    val result = m.invoke(target)
      .asInstanceOf[collection.Map[String, (TProtocol, Int) => Future[Array[Byte]]]]
    m.setAccessible(accessible)
    result
  }

  def thriftFinagleServiceFunctionMap(target: AnyRef) = {
    val f = target.getClass.getDeclaredField("functionMap")
    val accessible = f.isAccessible
    f.setAccessible(true)
    val functionMap = f.get(target)
      .asInstanceOf[java.util.Map[String, com.twitter.util.Function2[TProtocol, Integer, Future[Array[Byte]]]]]
      .asScala
      .mapValues(function => (prot: TProtocol, seqId: Int) => function(prot, seqId))
    f.setAccessible(accessible)
    functionMap
  }

  final def apply(request: Array[Byte]): Future[Array[Byte]] = {
    val inputTransport = new TMemoryInputTransport(request)
    val iprot = protocolFactory.getProtocol(inputTransport)
    try {
      val msg = iprot.readMessageBegin()
      val index = msg.name.indexOf(TMultiplexedProtocol.SEPARATOR)
      if (index == -1) {
        exception(msg.name, msg.seqid, TApplicationException.PROTOCOL_ERROR,
          s"This is a multiplexed service, with available service names: [${serviceMap.keys.mkString(", ")}]")
      } else {
        val serviceName = msg.name.substring(0, index)
        val functionName = msg.name.substring(index + 1)
        val functionMap = serviceMap.get(serviceName)
        val func = functionMap.flatMap(_.get(functionName))
        func match {
          case Some(fn) =>
            fn(iprot, msg.seqid)
          case _ =>
            TProtocolUtil.skip(iprot, TType.STRUCT)
            val errorMsg = if (functionMap.isEmpty) {
              s"""Invalid service name: ${serviceName}, available serivce names: [${serviceMap.keys.mkString(", ")}]"""
            } else {
              s"""Invalid method name: ${functionName}, available method names: [${functionMap.get.keys.mkString(", ")}]"""
            }
            exception(msg.name, msg.seqid, TApplicationException.UNKNOWN_METHOD, errorMsg)
        }
      }
    } catch {
      case e: Throwable => Future.exception(e)
    }
  }

  private[this] def exception(name: String, seqid: Int, code: Int, message: String): Future[Array[Byte]] = {
    try {
      val x = new TApplicationException(code, message)
      val memoryBuffer = reusableBuffer
      try {
        val oprot = protocolFactory.getProtocol(memoryBuffer)
        oprot.writeMessageBegin(new TMessage(name, TMessageType.EXCEPTION, seqid))
        x.write(oprot)
        oprot.writeMessageEnd()
        oprot.getTransport().flush()
        Future.value(Arrays.copyOfRange(memoryBuffer.getArray(), 0, memoryBuffer.length()))
      } finally {
        resetBuffer(memoryBuffer)
      }
    } catch {
      case e: Exception => Future.exception(e)
    }
  }

  private[this] val tlReusableBuffer = new ThreadLocal[TReusableMemoryTransport] {
    override def initialValue() = TReusableMemoryTransport(512)
  }

  private[this] def reusableBuffer: TReusableMemoryTransport = {
    val buf = tlReusableBuffer.get()
    buf.reset()
    buf
  }

  private[this] def resetBuffer(trans: TReusableMemoryTransport) {
    if (trans.currentCapacity > maxThriftBufferSize) {
      tlReusableBuffer.remove()
    }
  }
}
