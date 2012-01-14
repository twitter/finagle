package com.twitter.finagle.redis
package protocol

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import util.StringToChannelBuffer
import scala.collection.immutable.WrappedString

private[redis] object RedisCodec {
  object NilValue extends WrappedString("nil") {
    def getBytes(charset: String = "UTF-8") = Array[Byte]()
    def getBytes = Array[Byte]()
  }

  val STATUS_REPLY      = '+'
  val ERROR_REPLY       = '-'
  val INTEGER_REPLY     = ':'
  val BULK_REPLY        = '$'
  val MBULK_REPLY       = '*'

  val ARG_COUNT_MARKER  = '*'
  val ARG_SIZE_MARKER   = '$'

  val TOKEN_DELIMITER   = ' '
  val EOL_DELIMITER     = "\r\n"

  val NIL_VALUE         = NilValue
  val NIL_VALUE_BA      = NilValue.getBytes

  def toUnifiedFormat(args: List[Array[Byte]], includeHeader: Boolean = true) = {
    val buffer = ChannelBuffers.dynamicBuffer()
    includeHeader match {
      case true =>
        val argHeader = "%c%d%s".format(ARG_COUNT_MARKER, args.length, EOL_DELIMITER)
        buffer.writeBytes(argHeader.getBytes)
      case false =>
    }
    args.foreach { arg =>
      if (arg.length == 0) {
        buffer.writeBytes("%c-1%s".format(ARG_SIZE_MARKER, EOL_DELIMITER).getBytes)
      } else {
        val sizeHeader = "%c%d%s".format(ARG_SIZE_MARKER, arg.length, EOL_DELIMITER)
        buffer.writeBytes(sizeHeader.getBytes)
        buffer.writeBytes(arg)
        buffer.writeBytes(EOL_DELIMITER.getBytes)
      }
    }
    buffer
  }
  def toInlineFormat(args: List[String]) = {
    StringToChannelBuffer(args.mkString(TOKEN_DELIMITER.toString) + EOL_DELIMITER)
  }
}
abstract class RedisMessage {
  def toChannelBuffer: ChannelBuffer
  def toByteArray: Array[Byte] = toChannelBuffer.array
}
