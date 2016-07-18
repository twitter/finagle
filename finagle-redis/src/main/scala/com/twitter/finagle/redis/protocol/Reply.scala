package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.ServerError
import com.twitter.finagle.redis.naggati.ProtocolError
import com.twitter.finagle.redis.util._
import com.twitter.io.Buf
import org.jboss.netty.buffer.ChannelBuffer

object RequireServerProtocol extends ErrorConversion {
  override def getException(msg: String) = new ServerError(msg)
}

sealed abstract class Reply extends RedisMessage {
  override def toChannelBuffer: ChannelBuffer =
    throw new UnsupportedOperationException("Does not support server-side encoding")
}

sealed abstract class SingleLineReply extends Reply // starts with +,-, or :
sealed abstract class MultiLineReply extends Reply

case object NoReply extends Reply

case class StatusReply(message: String) extends SingleLineReply {
  RequireServerProtocol(message != null && message.length > 0, "StatusReply had empty message")
}

case class ErrorReply(message: String) extends SingleLineReply {
  RequireServerProtocol(message != null && message.length > 0, "ErrorReply had empty message")
}

case class IntegerReply(id: Long) extends SingleLineReply

case class BulkReply(message: Buf) extends MultiLineReply

case object EmptyBulkReply extends MultiLineReply

case class MBulkReply(messages: List[Reply]) extends MultiLineReply {
  RequireServerProtocol(
    messages != null && messages.nonEmpty,
    "Multi-BulkReply had empty message list")
}

case object EmptyMBulkReply extends MultiLineReply

case object NilMBulkReply extends MultiLineReply

class ReplyCodec extends UnifiedProtocolCodec {
  import com.twitter.finagle.redis.naggati.Stages._
  import com.twitter.finagle.redis.naggati.NextStep
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
      case empty if empty < 0 => emit(EmptyBulkReply)
      case replySz => readBytes(replySz) { bytes =>
        readBytes(2) { eol =>
          if (eol(0) != '\r' || eol(1) != '\n') {
            throw new ServerError("Expected EOL after line data and didn't find it")
          }
          emit(BulkReply(Buf.ByteArray.Owned(bytes)))
        }
      }
    }
  }

  def decodeMBulkReply(argCount: Long) =
    decodeMBulkLines(argCount, Nil, Nil)

  def decodeMBulkLines(i: Long, stack: List[(Long, List[Reply])], lines: List[Reply]): NextStep = {
    if (i <= 0) {
      val reply = (i, lines) match {
        case (i, _) if i < 0 => NilMBulkReply
        case (0, Nil) => EmptyMBulkReply
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
              decodeMBulkLines(i - 1, stack, EmptyBulkReply :: lines)
            } else {
              readBytes(size) { byteArray =>
                readBytes(2) { eol =>
                  if (eol(0) != '\r' || eol(1) != '\n') {
                    throw new ProtocolError("Expected EOL after line data and didn't find it")
                  }
                  decodeMBulkLines(i - 1, stack,
                    BulkReply(Buf.ByteArray.Owned(byteArray)) :: lines)
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