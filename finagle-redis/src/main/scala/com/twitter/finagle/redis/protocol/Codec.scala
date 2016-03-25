package com.twitter.finagle.redis.protocol

import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.redis.util.{StringToBuf, StringToChannelBuffer}
import com.twitter.io.{ConcatBuf, Buf}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import scala.collection.immutable.WrappedString

private[redis] object RedisCodec {
  object NilValue extends WrappedString("nil") {
    def getBytes(charset: String = "UTF_8") = Array[Byte]()
    def getBytes = Array[Byte]()
  }

  val STATUS_REPLY        = '+'
  val ERROR_REPLY         = '-'
  val INTEGER_REPLY       = ':'
  val BULK_REPLY          = '$'
  val MBULK_REPLY         = '*'

  val ARG_COUNT_MARKER    = '*'
  val ARG_SIZE_MARKER     = '$'

  val TOKEN_DELIMITER     = ' '
  val EOL_DELIMITER       = "\r\n"

  val NIL_VALUE           = NilValue
  val NIL_VALUE_BA        = ChannelBuffers.EMPTY_BUFFER


  val STATUS_REPLY_BA     = StringToChannelBuffer("+")
  val ERROR_REPLY_BA      = StringToChannelBuffer("-")
  val INTEGER_REPLY_BA    = StringToChannelBuffer(":")
  val BULK_REPLY_BA       = StringToChannelBuffer("$")
  val MBULK_REPLY_BA      = StringToChannelBuffer("*")

  val ARG_COUNT_MARKER_BA = MBULK_REPLY_BA
  val ARG_SIZE_MARKER_BA  = BULK_REPLY_BA

  val NIL_BULK_REPLY_BA     = StringToChannelBuffer("$-1")
  val EMPTY_MBULK_REPLY_BA  = StringToChannelBuffer("*0")
  val NIL_MBULK_REPLY_BA    = StringToChannelBuffer("*-1")

  val TOKEN_DELIMITER_BA  = StringToChannelBuffer(" ")
  val EOL_DELIMITER_BA    = StringToChannelBuffer(EOL_DELIMITER)

  val POS_INFINITY_BA     = StringToChannelBuffer("+inf")
  val NEG_INFINITY_BA     = StringToChannelBuffer("-inf")

  val BULK_REPLY_BUF: Buf       = StringToBuf("$")
  val MBULK_REPLY_BUF: Buf      = StringToBuf("*")

  val ARG_COUNT_MARKER_BUF: Buf = MBULK_REPLY_BUF
  val ARG_SIZE_MARKER_BUF: Buf  = BULK_REPLY_BUF

  val EOL_DELIMITER_BUF: Buf    = StringToBuf(EOL_DELIMITER)

  val NIL_VALUE_BUF: Buf        = Buf.Empty


  def toUnifiedFormat(args: Seq[ChannelBuffer], includeHeader: Boolean = true): ChannelBuffer = {
    val bufArgs = args.map(ChannelBufferBuf.Owned(_))
    ChannelBufferBuf.Owned.extract(toUnifiedBuf(bufArgs, includeHeader))
  }

  def bufToUnifiedChannelBuffer(args: Seq[Buf], includeHeader: Boolean = true): ChannelBuffer =
    ChannelBufferBuf.Owned.extract(toUnifiedBuf(args, includeHeader))

  def toUnifiedBuf(args: Seq[Buf], includeHeader: Boolean = true): Buf = {
    val header: Vector[Buf] =
      if (!includeHeader) Vector.empty else {
        Vector(
          ARG_COUNT_MARKER_BUF,
          StringToBuf(args.length.toString),
          EOL_DELIMITER_BUF)
      }
    val bufs = args.flatMap { arg =>
      Vector(
        ARG_SIZE_MARKER_BUF,
        StringToBuf(arg.length.toString),
        EOL_DELIMITER_BUF,
        arg,
        EOL_DELIMITER_BUF)
    }
    ConcatBuf(header ++ bufs)
  }
}

abstract class RedisMessage {
  def toChannelBuffer: ChannelBuffer
  def toByteArray: Array[Byte] = toChannelBuffer.array
}
