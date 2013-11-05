package com.twitter.finagle.exp.swift

import org.apache.thrift.protocol._
import org.apache.thrift.transport._
import java.util.Arrays

object Util {
  def newMessage(name: String, tpe: Byte)(writeArgs: TProtocol => Unit) = {
    val buf = new TMemoryBuffer(32)
    val out = new TBinaryProtocol(buf)
    out.writeMessageBegin(new TMessage(name, tpe, 0))
    out.writeStructBegin(new TStruct(name))
    writeArgs(out)
    out.writeFieldStop()
    out.writeStructEnd()
    out.writeMessageEnd()
    Arrays.copyOfRange(buf.getArray(), 0, buf.length())
  }
}
