package com.twitter.finagle.exp.swift

import com.facebook.swift.codec.ThriftCodec
import com.facebook.swift.codec.internal.{TProtocolReader, TProtocolWriter}
import com.google.common.base.Defaults
import com.google.common.primitives.Primitives
import com.twitter.finagle.Service
import com.twitter.util.Future
import java.util.Arrays
import org.apache.thrift.TApplicationException
import org.apache.thrift.protocol.{TProtocol, TBinaryProtocol, TMessage, TMessageType}
import org.apache.thrift.transport.{TMemoryBuffer, TMemoryInputTransport}
import com.google.common.reflect.TypeToken

/**
 * SwiftService is given a Swift-annotated processor, to which
 * it dispatches message frames. SwiftService multiplexes over 
 * several MethodDispatchers.
 */
class SwiftService(processor: Object) 
    extends Service[Array[Byte], Array[Byte]] {

  private[this] val sym = ServiceSym(processor.getClass)
  private[this] val methods = Map() ++ (
    for (m@MethodSym(name, _, _, _, _) <- sym.methods) yield 
      name -> new MethodDispatcher(m, processor))

  def apply(req: Array[Byte]): Future[Array[Byte]] = {
    val in = new TBinaryProtocol(new TMemoryInputTransport(req))
    val msg = in.readMessageBegin()
    val name = msg.name
    val seqid = msg.seqid
    val m = methods.get(name) getOrElse {
      return Future.exception(new TApplicationException(
        TApplicationException.UNKNOWN_METHOD, 
        "Unknown method "+name))
    }

    m(in) map { write =>
      val buf = new TMemoryBuffer(32)
      val out = new TBinaryProtocol(buf)
      out.writeMessageBegin(new TMessage(name, TMessageType.REPLY, seqid))
      write(out)
      out.writeMessageEnd()
      // Using slices would really help here.
      Arrays.copyOfRange(buf.getArray(), 0, buf.length())
    }
  }
}

/**
 * Responsible for dispatching to the method described by
 * the given symbol onto processor. This implements a 
 * Service -- SwiftService multiplexes over these.
 */
private class MethodDispatcher(sym: MethodSym, processor: Object) 
    extends Service[TProtocol, TProtocol => Unit] {

  private[this] val exceptions = (
    for {
      (id, thriftType) <- sym.exceptions
      k =TypeToken.of(thriftType.getJavaType()).getRawType()
      codec = ThriftCodecManager.getCodec(thriftType).asInstanceOf[ThriftCodec[Object]]
    } yield k -> (id, codec)
  ).toMap[Class[_], (Short, ThriftCodec[Object])]

  private[this] val nargs = sym.args.length
  private[this] val pos = Map() ++ (
    for ((ArgSym(_, id, _), pos) <- sym.args.zipWithIndex) yield (id -> pos))
  private[this] val codec = Map[Short, ThriftCodec[_]]() ++ (
    for (ArgSym(_, id, thriftType) <- sym.args) yield
      (id -> ThriftCodecManager.getCodec(thriftType))
  )
  private[this] val resultCodec = ThriftCodecManager.getCodec(sym.returnType)
    .asInstanceOf[ThriftCodec[Object]]

  private[this] def readArgs(in: TProtocol) = {
    val args = new Array[Object](nargs)
    val prot = new TProtocolReader(in)
    
    prot.readStructBegin()
    while (prot.nextField()) {
      val id = prot.getFieldId()
      codec.get(id) match {
        case None =>
          prot.skipFieldData()
        case Some(c) =>
          args(pos(id)) = prot.readField(c)
      }
    }
    prot.readStructEnd()

    // Now fill in unsupplied args with defaults.
    for ((ArgSym(_, _, thriftType), i) <- sym.args.zipWithIndex if args(i) == null) {
      thriftType.getJavaType() match {
        case tpe: Class[_] =>
          args(i) = Defaults.defaultValue(Primitives.unwrap(tpe)
            .asInstanceOf[Class[Object]])
        case _ => // ignore
      }
    }
    args
  }

  private[this] def invoke(args: Array[Object]): Future[Object] =
    sym.method.invoke(processor, args:_*).asInstanceOf[Future[Object]]

  def apply(in: TProtocol): Future[TProtocol => Unit] =
    invoke(readArgs(in)) map { res => out: TProtocol =>
      val prot = new TProtocolWriter(out)
      prot.writeStructBegin(sym.name+"_result")
      prot.writeField("success"/*field name*/, 0/*field id*/, resultCodec, res)
      prot.writeStructEnd()
    } handle {
      case e if exceptions contains e.getClass => out: TProtocol =>
        val (id, codec) = exceptions(e.getClass)
        val prot = new TProtocolWriter(out)
        prot.writeStructBegin(sym.name+"_result")
        prot.writeField("exception", id, codec, e)
        prot.writeStructEnd()
    }
}
