package com.twitter.finagle.exp.swift

import com.facebook.swift.codec.ThriftCodec
import com.facebook.swift.codec.internal.{TProtocolReader, TProtocolWriter} 
import com.twitter.finagle.Service
import com.twitter.finagle.thrift.ThriftClientRequest
import com.twitter.util.Future
import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.util.Arrays
import org.apache.thrift.TApplicationException
import org.apache.thrift.protocol.{TBinaryProtocol, TMessageType, TMessage}
import org.apache.thrift.transport.{TMemoryBuffer, TMemoryInputTransport} 

object SwiftProxy {
  /**
   * Given a service, create a `T`-typed client that dispatches
   * thrift-encoded messages on it. `T` must be Swift-annotated.
   */
  def newClient[T: ClassManifest](service: Service[ThriftClientRequest, Array[Byte]]): T = {
    val k = implicitly[ClassManifest[T]].erasure
    val sym = ServiceSym(k)
    Proxy.newProxyInstance(
      k.getClassLoader(),
      Array(k),
      new ProxyHandler(sym, service)
    ).asInstanceOf[T]
  }
}

private class ProxyHandler(
    sym: ServiceSym, 
    service: Service[ThriftClientRequest, Array[Byte]]) 
extends InvocationHandler {

  private[this] val codecs = Map() ++ (
    for (s@MethodSym(_, m, _, _, _) <- sym.methods)
      yield m -> new MethodCodec(s)
  )

  override def invoke(p: Object, m: Method, args: Array[Object]): Object = {
    if (m.getDeclaringClass() == classOf[Object]) {
      return m.getName() match {
        case "toString" => "Service("+sym+")"
        case "equals" => 
          val eq = equals(Proxy.getInvocationHandler(args(0)))
          new java.lang.Boolean(eq)
        case "hashCode" => new java.lang.Integer(hashCode())
        case _ => throw new UnsupportedOperationException
      }
    }

    codecs.get(m) match {
      case Some(codec) =>
        val encoded = codec.encode(args)
        service(new ThriftClientRequest(encoded, false)) map codec.decode
      case None =>
        val exc = new TApplicationException(
          TApplicationException.UNKNOWN_METHOD, 
          "Unknown method "+m)
        Future.exception(exc)
    }
  }
}

/**
 * Given a method symbol, a codec serializes and
 * deserializes argument structures.
 */
class MethodCodec(sym: MethodSym) {
  private[this] val returnCodec =
    ThriftCodecManager.getCodec(sym.returnType)
  private[this] val exceptions = Map() ++ (
    sym.exceptions map { case (id, thriftType) =>
      id -> ThriftCodecManager.getCodec(thriftType).asInstanceOf[ThriftCodec[Object]]
    }
  )

  def encode(args: Array[Object]): Array[Byte] = {
    val buf = new TMemoryBuffer(32)
    val out = new TBinaryProtocol(buf)
    out.writeMessageBegin(new TMessage(sym.name, TMessageType.CALL, 0))
    val writer = new TProtocolWriter(out)
    writer.writeStructBegin(sym.name+"_args")
    for (i <- Range(0, args.size)) {
      val ArgSym(name, id, thriftType) = sym.args(i)
      val codec = ThriftCodecManager
        .getCodec(thriftType).asInstanceOf[ThriftCodec[Object]]
      writer.writeField(name, id, codec, args(i))
    }
    writer.writeStructEnd()
    out.writeMessageEnd()
    Arrays.copyOfRange(buf.getArray(), 0, buf.length())
  }

  def decode(bytes: Array[Byte]): Object = {
    val buf = new TMemoryInputTransport(bytes)
    val in = new TBinaryProtocol(buf)
    val msg = in.readMessageBegin()
    if (msg.`type` == TMessageType.EXCEPTION)
      throw TApplicationException.read(in)

    require(msg.`type` == TMessageType.REPLY)
    // todo: check  method name, seqid, etc.

    val reader = new TProtocolReader(in)
    reader.readStructBegin()
    while (reader.nextField()) {
      reader.getFieldId() match {
        case 0 => 
          return reader.readField(returnCodec)
        case id if exceptions contains id => 
          throw reader.readField(exceptions(id)).asInstanceOf[Exception]
        case _ => 
          reader.skipFieldData()
        }
    }

    throw new TApplicationException(
      TApplicationException.MISSING_RESULT, 
      sym.name + " failed: unknown result")
  }
}
