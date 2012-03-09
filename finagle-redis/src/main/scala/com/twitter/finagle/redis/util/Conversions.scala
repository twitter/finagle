package com.twitter.finagle.redis
package util

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

trait ErrorConversion {
  def getException(msg: String): Throwable

  def apply(requirement: Boolean, message: String = "Prerequisite failed") {
    if (!requirement) {
      throw getException(message)
    }
  }
  def safe[T](fn: => T): T = {
    try {
      fn
    } catch {
      case e: Throwable => throw getException(e.getMessage)
    }
  }
}

object BytesToString {
  def apply(arg: Array[Byte], charset: String = "UTF-8") = new String(arg, charset)
  def fromList(args: List[Array[Byte]], charset: String = "UTF-8") = args.map { arg =>
    BytesToString(arg, charset)
  }
}
object StringToBytes {
  def apply(arg: String, charset: String = "UTF-8") = arg.getBytes(charset)
  def fromList(args: List[String], charset: String = "UTF-8") = args.map { arg =>
    arg.getBytes(charset)
  }
}
object StringToChannelBuffer {
  def apply(string: String, charset: String = "UTF-8") = {
    ChannelBuffers.wrappedBuffer(string.getBytes(charset))
  }
}
object NumberFormat {
  import com.twitter.naggati.ProtocolError
  def toDouble(arg: String): Double = {
    try {
      arg.toDouble
    } catch {
      case e: Throwable => throw new ProtocolError("Unable to convert %s to Double".format(arg))
    }
  }
  def toFloat(arg: String): Float = {
    try {
      arg.toFloat
    } catch {
      case e: Throwable => throw new ProtocolError("Unable to convert %s to Float".format(arg))
    }
  }
  def toInt(arg: String): Int = {
    try {
      arg.toInt
    } catch {
      case e: Throwable => throw new ProtocolError("Unable to convert %s to Int".format(arg))
    }
  }
  def toLong(arg: String): Long = {
    try {
      arg.toLong
    } catch {
      case e: Throwable => throw new ProtocolError("Unable to convert %s to Long".format(arg))
    }
  }
}

