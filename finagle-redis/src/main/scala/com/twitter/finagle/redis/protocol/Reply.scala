package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.ServerError
import com.twitter.finagle.redis.util._
import com.twitter.io.Buf

object RequireServerProtocol extends ErrorConversion {
  override def getException(msg: String): Exception = ServerError(msg)
}

sealed abstract class Reply
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
    "Multi-BulkReply had empty message list"
  )
}

case object EmptyMBulkReply extends MultiLineReply
case object NilMBulkReply extends MultiLineReply

object Reply {

  val EOL = Buf.Utf8("\r\n")
  val STATUS_REPLY = Buf.Utf8("+")
  val ERROR_REPLY = Buf.Utf8("-")
  val INTEGER_REPLY = Buf.Utf8(":")
  val BULK_REPLY = Buf.Utf8("$")
  val MBULK_REPLY = Buf.Utf8("*")

  import Stage.NextStep

  private[this] val decodeStatus =
    Stage.readLine(line => NextStep.Emit(StatusReply(line)))

  private[this] val decodeError =
    Stage.readLine(line => NextStep.Emit(ErrorReply(line)))

  private[this] val decodeInteger =
    Stage.readLine(line => RequireServerProtocol.safe(NextStep.Emit(IntegerReply(line.toLong))))

  private[this] val decodeBulk =
    Stage.readLine { line =>
      val num = RequireServerProtocol.safe(line.toInt)

      if (num < 0) NextStep.Emit(EmptyBulkReply)
      else
        NextStep.Goto(Stage.readBytes(num) { bytes =>
          NextStep.Goto(Stage.readBytes(2) {
            case EOL => NextStep.Emit(BulkReply(bytes))
            case _ => throw ServerError("Expected EOL after line data and didn't find it")
          })
        })
    }

  private[this] val decodeMBulk =
    Stage.readLine { line =>
      val num = RequireServerProtocol.safe(line.toLong)

      if (num < 0) NextStep.Emit(NilMBulkReply)
      else if (num == 0) NextStep.Emit(EmptyMBulkReply)
      else NextStep.Accumulate(num, MBulkReply.apply)
    }

  private[redis] val decode = Stage.readBytes(1) {
    case STATUS_REPLY => NextStep.Goto(decodeStatus)
    case ERROR_REPLY => NextStep.Goto(decodeError)
    case INTEGER_REPLY => NextStep.Goto(decodeInteger)
    case BULK_REPLY => NextStep.Goto(decodeBulk)
    case MBULK_REPLY => NextStep.Goto(decodeMBulk)
    case Buf.Utf8(rep) =>
      throw ServerError(s"Unknown response format($rep) found")
  }
}
