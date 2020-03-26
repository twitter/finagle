package com.twitter.finagle.redis.util

import com.twitter.finagle.redis.protocol._
import com.twitter.io.Buf
import java.nio.charset.{Charset, StandardCharsets}

trait ErrorConversion {
  def getException(msg: String): Throwable

  def apply(requirement: Boolean, message: String = "Prerequisite failed"): Unit = {
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
  def apply(arg: Array[Byte], charset: Charset = StandardCharsets.UTF_8) = new String(arg, charset)

  def fromList(args: Seq[Array[Byte]], charset: Charset = StandardCharsets.UTF_8) =
    args.map { arg => BytesToString(arg, charset) }

  def fromTuples(args: Seq[(Array[Byte], Array[Byte])], charset: Charset = StandardCharsets.UTF_8) =
    args map { arg => (BytesToString(arg._1), BytesToString(arg._2)) }

  def fromTuplesWithDoubles(
    args: Seq[(Array[Byte], Double)],
    charset: Charset = StandardCharsets.UTF_8
  ) =
    args map { arg => (BytesToString(arg._1, charset), arg._2) }
}

object StringToBytes {
  def apply(arg: String, charset: Charset = StandardCharsets.UTF_8) = arg.getBytes(charset)
  def fromList(args: List[String], charset: Charset = StandardCharsets.UTF_8) =
    args.map { arg => arg.getBytes(charset) }
}

object StringToBuf {
  def apply(string: String): Buf = Buf.Utf8(string)
}

object BufToString {
  def apply(buf: Buf): String = Buf.Utf8.unapply(buf).get
}

object ReplyFormat {
  def toString(items: List[Reply]): List[String] = {
    items flatMap {
      case BulkReply(message) => List(BufToString(message))
      case EmptyBulkReply => EmptyBulkReplyString
      case IntegerReply(id) => List(id.toString)
      case StatusReply(message) => List(message)
      case ErrorReply(message) => List(message)
      case MBulkReply(messages) => ReplyFormat.toString(messages)
      case EmptyMBulkReply => EmptyMBulkReplyString
      case _ => Nil
    }
  }

  def toBuf(items: List[Reply]): List[Buf] = {
    items flatMap {
      case BulkReply(message) => List(message)
      case EmptyBulkReply => EmptyBulkReplyChannelBuffer
      case IntegerReply(id) => List(Buf.ByteArray.Owned(Array(id.toByte)))
      case StatusReply(message) => List(Buf.Utf8(message))
      case ErrorReply(message) => List(Buf.Utf8(message))
      case MBulkReply(messages) => ReplyFormat.toBuf(messages)
      case EmptyMBulkReply => EmptyBulkReplyChannelBuffer
      case _ => Nil
    }
  }

  private val EmptyBulkReplyString: List[String] = List("nil")
  private val EmptyMBulkReplyString: List[String] = List("")
  private val EmptyBulkReplyChannelBuffer: List[Buf] = List(Buf.Empty)
}
