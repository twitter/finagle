package com.twitter.finagle.redis
package protocol

import util._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

object RequireServerProtocol extends ErrorConversion {
  override def getException(msg: String) = new ServerError(msg)
}

class ServerError(message: String) extends Exception(message)

sealed abstract class Reply extends RedisMessage
sealed abstract class SingleLineReply extends Reply { // starts with +,-, or :
  import RedisCodec.EOL_DELIMITER

  def getMessageTuple(): (Char,String)
  override def toChannelBuffer = {
    val (c,s) = getMessageTuple
    StringToChannelBuffer("%c%s%s".format(c,s,EOL_DELIMITER))
  }
}
sealed abstract class MultiLineReply extends Reply

case class StatusReply(message: String) extends SingleLineReply {
  RequireServerProtocol(message != null && message.length > 0, "StatusReply had empty message")
  override def getMessageTuple() = (RedisCodec.STATUS_REPLY, message)
}
case class ErrorReply(message: String) extends SingleLineReply {
  RequireServerProtocol(message != null && message.length > 0, "ErrorReply had empty message")
  override def getMessageTuple() = (RedisCodec.ERROR_REPLY, message)
}
case class IntegerReply(id: Int) extends SingleLineReply {
  override def getMessageTuple() = (RedisCodec.INTEGER_REPLY, id.toString)
}

case class BulkReply(message: Array[Byte]) extends MultiLineReply {
  RequireServerProtocol(message != null && message.length > 0, "BulkReply had empty message")

  import RedisCodec.{ARG_SIZE_MARKER, EOL_DELIMITER}

  override def toChannelBuffer = {
    val mlen = message.length
    val exlen = 1 + 2 + 2 // 1 byte for marker, 2 bytes for first EOL, 2 for second
    val header = "%c%d%s".format(ARG_SIZE_MARKER, mlen, EOL_DELIMITER)
    val buffer = ChannelBuffers.dynamicBuffer(mlen + exlen)
    buffer.writeBytes(header.getBytes)
    buffer.writeBytes(message)
    buffer.writeBytes(EOL_DELIMITER.getBytes)
    buffer
  }
}
case class EmptyBulkReply() extends MultiLineReply {
  val message = RedisCodec.NIL_VALUE
  val messageBytes = StringToBytes(message)
  override def toChannelBuffer = RedisCodec.toInlineFormat(List("$-1"))
}

case class MBulkReply(messages: List[Array[Byte]]) extends MultiLineReply {
  RequireServerProtocol(
    messages != null && messages.length > 0,
    "Multi-BulkReply had empty message list")
  override def toChannelBuffer = RedisCodec.toUnifiedFormat(messages)
}
case class EmptyMBulkReply() extends MultiLineReply {
  val message = RedisCodec.NIL_VALUE
  val messageBytes = StringToBytes(message)
  override def toChannelBuffer = RedisCodec.toInlineFormat(List("*0"))
}

class ReplyCodec extends UnifiedProtocolCodec {
  import com.twitter.finagle.redis.naggati.{Emit, Encoder, NextStep}
  import com.twitter.finagle.redis.naggati.Stages._
  import RedisCodec._

  val decode = readBytes(1) { bytes =>
    bytes(0) match {
      case STATUS_REPLY =>
        readLine { line => emit(StatusReply(line)) }
      case ERROR_REPLY =>
        readLine { line => emit(ErrorReply(line)) }
      case INTEGER_REPLY =>
        readLine { line =>
          RequireServerProtocol.safe {
            emit(IntegerReply(NumberFormat.toInt(line)))
          }
        }
      case BULK_REPLY =>
        decodeBulkReply
      case MBULK_REPLY =>
        val doneFn = { lines: List[Array[Byte]] =>
          lines.length match {
            case empty if empty == 0 => EmptyMBulkReply()
            case n => MBulkReply(lines)
          }
        }
        RequireServerProtocol.safe {
          readLine { line => decodeUnifiedFormat(NumberFormat.toLong(line), doneFn) }
        }
      case b: Byte =>
        throw new ServerError("Unknown response format(%c) found".format(b.asInstanceOf[Char]))
    }
  }

  val encode = new Encoder[Reply] {
    def encode(obj: Reply) = Some(obj.toChannelBuffer)
  }

  def decodeBulkReply = readLine { line =>
    RequireServerProtocol.safe {
      NumberFormat.toInt(line)
    } match {
      case empty if empty < 1 => emit(EmptyBulkReply())
      case replySz => readBytes(replySz) { bytes =>
        readBytes(2) { eol =>
          if (eol(0) != '\r' || eol(1) != '\n') {
            throw new ServerError("Expected EOL after line data and didn't find it")
          }
          emit(BulkReply(bytes))
        } //readBytes
      } // readBytes
    } // match
  } // decodeBulkReply

}
