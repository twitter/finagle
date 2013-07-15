package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.naggati.{NextStep, ProtocolError}
import com.twitter.finagle.redis.naggati.Stages._
import com.twitter.finagle.redis.protocol.RedisCodec._
import com.twitter.finagle.redis.util._

trait UnifiedProtocolCodec {

  type ByteArrays = List[Array[Byte]]

  def decodeUnifiedFormat[T <: AnyRef](argCount: Long, doneFn: ByteArrays => T) =
    argCount match {
      case n if n < 0 => throw new ProtocolError("Invalid argument count specified")
      case n => decodeRequestLines(n, Nil, { lines => doneFn(lines) } )
    }

  def decodeRequestLines[T <: AnyRef](
    i: Long,
    lines: ByteArrays,
    doneFn: ByteArrays => T): NextStep =
  {
    if (i <= 0) {
      emit(doneFn(lines.reverse))
    } else {
      readLine { line =>
        val header = line(0)
        header match {
          case ARG_SIZE_MARKER =>
            val size = NumberFormat.toInt(line.drop(1))
            if (size < 1) {
              decodeRequestLines(i - 1, lines.+:(RedisCodec.NIL_VALUE_BA.array), doneFn)
            } else {
              readBytes(size) { byteArray =>
                readBytes(2) { eol =>
                  if (eol(0) != '\r' || eol(1) != '\n') {
                    throw new ProtocolError("Expected EOL after line data and didn't find it")
                  }
                  decodeRequestLines(i - 1, lines.+:(byteArray), doneFn)
                }
              }
            }
          case STATUS_REPLY =>
            decodeRequestLines(i - 1, lines.+:(line.drop(1).getBytes), doneFn)
          case ARG_COUNT_MARKER =>
            decodeRequestLines(line.drop(1).toLong, lines, doneFn)
          case INTEGER_REPLY =>
            decodeRequestLines(i - 1, lines.+:(line.drop(1).getBytes), doneFn)
          case ERROR_REPLY =>
            decodeRequestLines(i - 1, lines.+:(line.drop(1).getBytes), doneFn)
          case b: Char =>
            throw new ProtocolError("Expected size marker $, got " + b)
        } // header match
      } // readLine
    } // else
  } // decodeRequestLines
}
