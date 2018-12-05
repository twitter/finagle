package com.twitter.finagle.thrift

import com.twitter.finagle.{Service, Thrift}
import com.twitter.scrooge.TReusableMemoryTransport
import com.twitter.util.{Future, Try}
import java.util.Arrays
import org.apache.thrift.TApplicationException
import org.apache.thrift.protocol._
import org.apache.thrift.transport.TMemoryInputTransport
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

private[finagle] class MultiplexedFinagleService(
  services: Map[String, Service[Array[Byte], Array[Byte]]],
  defaultService: Option[String],
  protocolFactory: TProtocolFactory,
  maxThriftBufferSize: Int = Thrift.param.maxThriftBufferSize)
    extends Service[Array[Byte], Array[Byte]] {

  private val serviceMap = services.mapValues(getFunctionMap)

  private[this] def getFunctionMap(
    service: Service[Array[Byte], Array[Byte]]
  ): collection.Map[String, Service[(TProtocol, Int), Array[Byte]]] =
    Try {
      scroogeFinagleServiceFunctionMap(service)
    }.handle {
        case NonFatal(_) =>
          thriftFinagleServiceFunctionMap(service)
      }
      .handle {
        case NonFatal(e) =>
          throw new IllegalArgumentException(
            "%s cannot be multiplexed".format(service.getClass.getName),
            e
          )
      }
      .get

  /**
   * Get the function map from scrooge generated scala services.
   */
  private[this] def scroogeFinagleServiceFunctionMap[T](
    target: AnyRef
  ): collection.Map[String, Service[(TProtocol, Int), Array[Byte]]] = {
    val m = target.getClass.getMethod("serviceMap")
    val accessible = m.isAccessible
    m.setAccessible(true)
    val result = m
      .invoke(target)
      .asInstanceOf[collection.Map[String, Service[(TProtocol, Int), Array[Byte]]]]
    m.setAccessible(accessible)
    result
  }

  /**
   * Get the function map from scrooge generated java services.
   */
  private[this] def thriftFinagleServiceFunctionMap(
    target: AnyRef
  ): collection.Map[String, Service[(TProtocol, Int), Array[Byte]]] = {
    val f = target.getClass.getDeclaredField("serviceMap")
    val accessible = f.isAccessible
    f.setAccessible(true)
    val functionMap = f
      .get(target)
      .asInstanceOf[java.util.Map[String, Service[(TProtocol, Int), Array[Byte]]]]
      .asScala
    f.setAccessible(accessible)
    functionMap
  }

  final def apply(request: Array[Byte]): Future[Array[Byte]] = {
    val inputTransport = new TMemoryInputTransport(request)
    val iprot = protocolFactory.getProtocol(inputTransport)
    try {
      val msg = iprot.readMessageBegin()
      val index = msg.name.indexOf(TMultiplexedProtocol.SEPARATOR)
      if (index == -1 && defaultService.isEmpty) {
        exception(
          msg.name,
          msg.seqid,
          TApplicationException.PROTOCOL_ERROR,
          s"This is a multiplexed service, with available service names: [${serviceMap.keys.mkString(", ")}]"
        )
      } else {
        val serviceName = if (index == -1) defaultService.get else msg.name.substring(0, index)
        val functionName = msg.name.substring(index + 1)
        val functionMap = serviceMap.get(serviceName)
        val service = functionMap.flatMap(_.get(functionName))
        service match {
          case Some(svc) =>
            svc((iprot, msg.seqid))
          case _ =>
            TProtocolUtil.skip(iprot, TType.STRUCT)
            val errorMsg = if (functionMap.isEmpty) {
              s"""Invalid service name: ${serviceName}, available serivce names: [${serviceMap.keys
                .mkString(", ")}]"""
            } else {
              s"""Invalid method name: ${functionName}, available method names: [${functionMap.get.keys
                .mkString(", ")}]"""
            }
            exception(msg.name, msg.seqid, TApplicationException.UNKNOWN_METHOD, errorMsg)
        }
      }
    } catch {
      case NonFatal(e) => Future.exception(e)
    }
  }

  private[this] def exception(
    name: String,
    seqid: Int,
    code: Int,
    message: String
  ): Future[Array[Byte]] = {
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
      case NonFatal(e) => Future.exception(e)
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

  private[this] def resetBuffer(trans: TReusableMemoryTransport): Unit = {
    if (trans.currentCapacity > maxThriftBufferSize) {
      tlReusableBuffer.remove()
    }
  }
}
