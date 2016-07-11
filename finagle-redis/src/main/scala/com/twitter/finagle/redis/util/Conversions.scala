package com.twitter.finagle.redis.util

import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.protocol.Commands.trimList
import com.twitter.io.{Buf, Charsets}
import java.nio.charset.Charset
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
  def apply(arg: Array[Byte], charset: Charset = Charsets.Utf8) = new String(arg, charset)

  def fromList(args: Seq[Array[Byte]], charset: Charset = Charsets.Utf8) =
    args.map { arg => BytesToString(arg, charset) }

  def fromTuples(args: Seq[(Array[Byte], Array[Byte])], charset: Charset = Charsets.Utf8) =
    args map { arg => (BytesToString(arg._1), BytesToString(arg._2)) }

  def fromTuplesWithDoubles(args: Seq[(Array[Byte], Double)],
    charset: Charset = Charsets.Utf8) =
    args map { arg => (BytesToString(arg._1, charset), arg._2) }
}

object GetMonadArg {
  def apply(args: Seq[Array[Byte]], command: ChannelBuffer): ChannelBuffer =
    ChannelBuffers.wrappedBuffer(trimList(args, 1, CBToString(command))(0))

  def apply(args: Seq[Array[Byte]], command: Buf): Buf =
    Buf.ByteArray.Owned(trimList(args, 1, BufToString(command))(0))
}

object StringToBytes {
  def apply(arg: String, charset: Charset = Charsets.Utf8) = arg.getBytes(charset)
  def fromList(args: List[String], charset: Charset = Charsets.Utf8) =
    args.map { arg =>
      arg.getBytes(charset)
    }
}
object StringToChannelBuffer {
  def apply(string: String, charset: Charset = Charsets.Utf8) = {
    ChannelBuffers.wrappedBuffer(string.getBytes(charset))
  }
}

object StringToBuf {
  def apply(string: String): Buf = Buf.Utf8(string)
}

object BufToString {
  def apply(buf: Buf): String = Buf.Utf8.unapply(buf).get
}

object CBToString {
  def apply(arg: ChannelBuffer, charset: Charset = Charsets.Utf8) = {
    arg.toString(charset)
  }
  def fromList(args: Seq[ChannelBuffer], charset: Charset = Charsets.Utf8) =
    args.map { arg => CBToString(arg, charset) }

  def fromTuples(args: Seq[(ChannelBuffer, ChannelBuffer)], charset: Charset = Charsets.Utf8) =
    args map { arg => (CBToString(arg._1), CBToString(arg._2)) }

  def fromTuplesWithDoubles(args: Seq[(ChannelBuffer, Double)],
    charset: Charset = Charsets.Utf8) =
    args map { arg => (CBToString(arg._1, charset), arg._2) }
}
object NumberFormat {
  import com.twitter.finagle.redis.naggati.ProtocolError
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
object ReplyFormat {
  def toString(items: List[Reply]): List[String] = {
    items flatMap {
      case BulkReply(message)   => List(BufToString(message))
      case EmptyBulkReply()     => EmptyBulkReplyString
      case IntegerReply(id)     => List(id.toString)
      case StatusReply(message) => List(message)
      case ErrorReply(message)  => List(message)
      case MBulkReply(messages) => ReplyFormat.toString(messages)
      case EmptyMBulkReply()    => EmptyMBulkReplyString
      case _                    => Nil
    }
  }

  def toChannelBuffers(items: List[Reply]): List[ChannelBuffer] =
    toBuf(items).map(ChannelBufferBuf.Owned.extract(_))

  def toBuf(items: List[Reply]): List[Buf] = {
    items flatMap {
      case BulkReply(message)   => List(message)
      case EmptyBulkReply()     => EmptyBulkReplyChannelBuffer
      case IntegerReply(id)     => List(Buf.ByteArray.Owned(Array(id.toByte)))
      case StatusReply(message) => List(StringToBuf(message))
      case ErrorReply(message)  => List(StringToBuf(message))
      case MBulkReply(messages) => ReplyFormat.toBuf(messages)
      case EmptyMBulkReply()    => EmptyBulkReplyChannelBuffer
      case _                    => Nil
    }
  }

  private val EmptyBulkReplyString: List[String] = List(RedisCodec.NIL_VALUE.toString)
  private val EmptyMBulkReplyString: List[String] = List(BytesToString(RedisCodec.NIL_VALUE_BA.array))
  private val EmptyBulkReplyChannelBuffer: List[Buf] = List(RedisCodec.NIL_VALUE_BUF)
}
