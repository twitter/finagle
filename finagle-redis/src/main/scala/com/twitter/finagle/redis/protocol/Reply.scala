package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.naggati.ProtocolError
import com.twitter.finagle.redis.ServerError
import com.twitter.finagle.redis.util._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

object RequireServerProtocol extends ErrorConversion {
  override def getException(msg: String) = new ServerError(msg)
}

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
case class IntegerReply(id: Long) extends SingleLineReply {
  override def getMessageTuple() = (RedisCodec.INTEGER_REPLY, id.toString)
}

case class BulkReply(message: ChannelBuffer) extends MultiLineReply {
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(List(message), false)
}
case class EmptyBulkReply() extends MultiLineReply {
  val message = "$-1"
  override def toChannelBuffer =
    ChannelBuffers.wrappedBuffer(RedisCodec.NIL_BULK_REPLY_BA,
      RedisCodec.EOL_DELIMITER_BA)
}

case class MBulkReply(messages: List[Reply]) extends MultiLineReply {
  RequireServerProtocol(
    messages != null && messages.length > 0,
    "Multi-BulkReply had empty message list")
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(ReplyFormat.toChannelBuffers(messages))
}
case class EmptyMBulkReply() extends MultiLineReply {
  val message = "*0"
  override def toChannelBuffer =
    ChannelBuffers.wrappedBuffer(RedisCodec.EMPTY_MBULK_REPLY_BA,
      RedisCodec.EOL_DELIMITER_BA)
}
case class NilMBulkReply() extends MultiLineReply {
  val message = "*-1"
  override def toChannelBuffer =
    ChannelBuffers.wrappedBuffer(RedisCodec.NIL_MBULK_REPLY_BA,
      RedisCodec.EOL_DELIMITER_BA)
}

class ReplyCodec extends UnifiedProtocolCodec {
  import com.twitter.finagle.redis.naggati.{Encoder, NextStep}
  import com.twitter.finagle.redis.naggati.Stages._
  import RedisCodec._

  val encode = new Encoder[Reply] {
    def encode(obj: Reply) = Some(obj.toChannelBuffer)
  }

  val decode = readBytes(1) { bytes =>
    bytes(0) match {
      case STATUS_REPLY =>
        readLine { line => emit(StatusReply(line)) }
      case ERROR_REPLY =>
        readLine { line => emit(ErrorReply(line)) }
      case INTEGER_REPLY =>
        readLine { line =>
          RequireServerProtocol.safe {
            emit(IntegerReply(NumberFormat.toLong(line)))
          }
        }
      case BULK_REPLY =>
        decodeBulkReply
      case MBULK_REPLY =>
        RequireServerProtocol.safe {
          readLine { line => decodeMBulkReply(NumberFormat.toLong(line)) }
        }
      case b: Byte =>
        throw new ServerError("Unknown response format(%c) found".format(b.asInstanceOf[Char]))
    }
  }

  def decodeBulkReply = readLine { line =>
    RequireServerProtocol.safe {
      NumberFormat.toInt(line)
    } match {
      case empty if empty < 0 => emit(EmptyBulkReply())
      case replySz => readBytes(replySz) { bytes =>
        readBytes(2) { eol =>
          if (eol(0) != '\r' || eol(1) != '\n') {
            throw new ServerError("Expected EOL after line data and didn't find it")
          }
          emit(BulkReply(ChannelBuffers.wrappedBuffer(bytes)))
        }
      }
    }
  }

  def decodeMBulkReply(argCount: Long) =
    decodeMBulkLines(argCount, Nil, Nil)

  def decodeMBulkLines(i: Long, stack: List[(Long, List[Reply])], lines: List[Reply]): NextStep = {
    if (i <= 0) {
      val reply = (i, lines) match {
        case (i, _) if i < 0 => NilMBulkReply()
        case (0, Nil) => EmptyMBulkReply()
        case (0, lines) => MBulkReply(lines.reverse)
      }
      stack match {
        case Nil => emit(reply)
        case (i, lines) :: stack => decodeMBulkLines(i, stack, reply :: lines)
      }
    } else {
      readLine { line =>
        val header = line(0)
        header match {
          case ARG_SIZE_MARKER =>
            val size = NumberFormat.toInt(line.drop(1))
            if (size < 0) {
              decodeMBulkLines(i - 1, stack, EmptyBulkReply() :: lines)
            } else {
              readBytes(size) { byteArray =>
                readBytes(2) { eol =>
                  if (eol(0) != '\r' || eol(1) != '\n') {
                    throw new ProtocolError("Expected EOL after line data and didn't find it")
                  }
                  decodeMBulkLines(i - 1, stack,
                    BulkReply(ChannelBuffers.wrappedBuffer(byteArray)) :: lines)
                }
              }
            }
          case STATUS_REPLY =>
            decodeMBulkLines(i - 1, stack, StatusReply(BytesToString(
              line.drop(1).getBytes)) :: lines)
          case ARG_COUNT_MARKER =>
            decodeMBulkLines(line.drop(1).toLong, (i - 1, lines) :: stack, Nil)
          case INTEGER_REPLY =>
            decodeMBulkLines(i - 1, stack, IntegerReply(NumberFormat.toLong(
              BytesToString(line.drop(1).getBytes))) :: lines)
          case ERROR_REPLY =>
            decodeMBulkLines(i - 1, stack, ErrorReply(BytesToString(
              line.drop(1).getBytes)) :: lines)
          case b: Char =>
            throw new ProtocolError("Expected size marker $, got " + b)
        }
      }
    }
  }
}
