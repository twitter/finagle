package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.util.StringToChannelBuffer
import com.twitter.io.Buf
import org.jboss.netty.buffer.ChannelBuffers

private[redis] object RedisCodec {

  val STATUS_REPLY        = '+'
  val ERROR_REPLY         = '-'
  val INTEGER_REPLY       = ':'
  val BULK_REPLY          = '$'
  val MBULK_REPLY         = '*'

  val ARG_COUNT_MARKER    = '*'
  val ARG_SIZE_MARKER     = '$'

  val TOKEN_DELIMITER     = ' '
  val EOL_DELIMITER       = "\r\n"

  val NIL_VALUE           = "nil"
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

  val BULK_REPLY_BUF: Buf       = Buf.Utf8("$")
  val MBULK_REPLY_BUF: Buf      = Buf.Utf8("*")

  val ARG_COUNT_MARKER_BUF: Buf = MBULK_REPLY_BUF
  val ARG_SIZE_MARKER_BUF: Buf  = BULK_REPLY_BUF

  val EOL_DELIMITER_BUF: Buf    = Buf.Utf8(EOL_DELIMITER)

  val NIL_VALUE_BUF: Buf        = Buf.Empty
}