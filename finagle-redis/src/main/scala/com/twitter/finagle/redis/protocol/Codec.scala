package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.util.StringToChannelBuffer
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


  def toUnifiedFormat(args: Seq[ChannelBuffer], includeHeader: Boolean = true) = {
    val header = includeHeader match {
      case true =>
        Seq(ARG_COUNT_MARKER_BA, StringToChannelBuffer(args.length.toString), EOL_DELIMITER_BA)
      case false => Nil
    }
    val buffers = args.map({ arg =>
      Seq(
        ARG_SIZE_MARKER_BA,
        StringToChannelBuffer(arg.readableBytes.toString),
        EOL_DELIMITER_BA,
        arg,
        EOL_DELIMITER_BA
      )
    }).flatten
    ChannelBuffers.wrappedBuffer((header ++ buffers).toArray:_*)
  }

}

abstract class RedisMessage {
  def toChannelBuffer: ChannelBuffer
  def toByteArray: Array[Byte] = toChannelBuffer.array
}
