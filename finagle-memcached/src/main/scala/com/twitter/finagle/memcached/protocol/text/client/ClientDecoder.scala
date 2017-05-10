package com.twitter.finagle.memcached.protocol.text.client

import com.twitter.finagle.memcached.protocol.ServerError
import com.twitter.finagle.memcached.protocol.text._
import com.twitter.finagle.memcached.util.ParserUtils
import com.twitter.io.Buf

/**
 * Decodes Buf-encoded Responses into Decodings. Used by the client.
 *
 * @note Class contains mutable state. Not thread-safe.
 */
private[memcached] object ClientDecoder {
  private val END: Buf = Buf.Utf8("END")
  private val ITEM: Buf = Buf.Utf8("ITEM")
  private val STAT: Buf = Buf.Utf8("STAT")
  private val VALUE: Buf = Buf.Utf8("VALUE")

  private val EmptyValueLines: ValueLines = ValueLines(Seq.empty)

  // Constant for the length of a byte array that will contain a String representation of an Int,
  // which is used in the Decoder class when converting a Buf to an Int
  private val MaxLengthOfIntString = Int.MinValue.toString.length

  private val NeedMoreData: Null = null

  private sealed trait State
  private case object AwaitingResponse extends State
  private case class AwaitingResponseOrEnd(valuesSoFar: Seq[TokensWithData]) extends State
  private case class AwaitingStatsOrEnd(valuesSoFar: Seq[Tokens]) extends State
  private case class AwaitingData(
      valuesSoFar: Seq[TokensWithData],
      tokens: Seq[Buf],
      bytesNeeded: Int) extends State
}

private[finagle] class ClientDecoder extends Decoder {
  import ClientDecoder._

  private[this] var state: State = AwaitingResponse

  private[this] val awaitingResponseContinue: Seq[Buf] => Decoding = { tokens =>
    if (isEnd(tokens)) {
      EmptyValueLines
    } else if (isStats(tokens)) {
      awaitStatsOrEnd(Seq(Tokens(tokens)))
      NeedMoreData
    } else {
      Tokens(tokens)
    }
  }

  def decode(buffer: Buf): Decoding = {
    state match {
      case AwaitingResponse =>
        decodeLine(buffer, needsData, awaitData)(awaitingResponseContinue)

      case AwaitingStatsOrEnd(linesSoFar) =>
        decodeLine(buffer, needsData, awaitData) { tokens =>
          state = AwaitingResponse
          if (isEnd(tokens)) {
            StatLines(linesSoFar)
          } else if (isStats(tokens)) {
            awaitStatsOrEnd(linesSoFar :+ Tokens(tokens))
            NeedMoreData
          } else {
            throw new ServerError("Invalid reply from STATS command")
          }
        }
      case AwaitingData(valuesSoFar, tokens, bytesNeeded) =>
        decodeData(bytesNeeded, buffer) { data =>
          awaitResponseOrEnd(
            valuesSoFar :+
              TokensWithData(tokens, data)
          )
          NeedMoreData
        }
      case AwaitingResponseOrEnd(valuesSoFar) =>
        decodeLine(buffer, needsData, awaitData) { tokens =>
          state = AwaitingResponse
          if (isEnd(tokens)) {
            ValueLines(valuesSoFar)
          } else NeedMoreData
        }
    }
  }

  private[this] def awaitData(tokens: Seq[Buf], bytesNeeded: Int): Unit = {
    state match {
      case AwaitingResponse =>
        awaitData(Nil, tokens, bytesNeeded)
      case AwaitingResponseOrEnd(valuesSoFar) =>
        awaitData(valuesSoFar, tokens, bytesNeeded)
      case otherState => throw new IllegalStateException(
        s"Received data while in invalid state: $otherState")
    }
  }

  private[this] def awaitData(
    valuesSoFar: Seq[TokensWithData],
    tokens: Seq[Buf],
    bytesNeeded: Int
  ): Unit = {
    state = AwaitingData(valuesSoFar, tokens, bytesNeeded)
  }

  private[this] def awaitResponseOrEnd(valuesSoFar: Seq[TokensWithData]): Unit = {
    state = AwaitingResponseOrEnd(valuesSoFar)
  }

  private[this] def awaitStatsOrEnd(valuesSoFar: Seq[Tokens]): Unit = {
    state = AwaitingStatsOrEnd(valuesSoFar)
  }

  private[this] def isEnd(tokens: Seq[Buf]) =
    tokens.length == 1 && tokens.head == END

  private[this] def isStats(tokens: Seq[Buf]) =
    tokens.nonEmpty && (tokens.head == STAT || tokens.head == ITEM)

  private[this] val needsData: Seq[Buf] => Int = { tokens =>
    val responseName = tokens.head
    if (responseName == VALUE) {
      validateValueResponse(tokens)
      val dataLengthAsBuf = tokens(3)
      dataLengthAsBuf.write(byteArrayForBuf2Int, 0)
      ParserUtils.byteArrayStringToInt(byteArrayForBuf2Int, dataLengthAsBuf.length)
    } else -1
  }

  private[this] def validateValueResponse(args: Seq[Buf]): Unit = {
    if (args.length < 4) throw new ServerError("Too few arguments")
    if (args.length > 5) throw new ServerError("Too many arguments")
    if (args.length == 5 && !ParserUtils.isDigits(args(4))) throw new ServerError("CAS must be a number")
    if (!ParserUtils.isDigits(args(3))) throw new ServerError("Bytes must be number")
  }
}
