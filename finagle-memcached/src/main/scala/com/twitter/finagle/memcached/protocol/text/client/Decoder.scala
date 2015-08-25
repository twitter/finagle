package com.twitter.finagle.memcached.protocol.text.client

import com.twitter.finagle.memcached.protocol.ServerError
import com.twitter.finagle.memcached.protocol.text._
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.memcached.util.ParserUtils
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.util.StateMachine
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel._

object Decoder {
  private val END   = "END": ChannelBuffer
  private val ITEM  = "ITEM": ChannelBuffer
  private val STAT  = "STAT": ChannelBuffer
  private val VALUE = "VALUE": ChannelBuffer

  private val EmptyValueLines = ValueLines(Seq.empty)

  private val NeedMoreData: Decoding = null
}

class Decoder extends AbstractDecoder with StateMachine {
  import Decoder._

  case object AwaitingResponse extends State
  case class AwaitingResponseOrEnd(valuesSoFar: Seq[TokensWithData]) extends State
  case class AwaitingStatsOrEnd(valuesSoFar: Seq[Tokens]) extends State
  case class AwaitingData(
      valuesSoFar: Seq[TokensWithData],
      tokens: Seq[ChannelBuffer],
      bytesNeeded: Int)
    extends State

  final protected[memcached] def start() {
    state = AwaitingResponse
  }

  private[this] val awaitingResponseContinue: Seq[ChannelBuffer] => Decoding = { tokens =>
    if (isEnd(tokens)) {
      EmptyValueLines
    } else if (isStats(tokens)) {
      awaitStatsOrEnd(Seq(Tokens(tokens.map(ChannelBufferBuf.Owned(_)))))
      NeedMoreData
    } else {
      Tokens(tokens.map(ChannelBufferBuf.Owned(_)))
    }
  }

  def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer): Decoding = {
    state match {
      case AwaitingResponse =>
        decodeLine(buffer, needsData)(awaitingResponseContinue)

      case AwaitingStatsOrEnd(linesSoFar) =>
        decodeLine(buffer, needsData) { tokens =>
          if (isEnd(tokens)) {
            StatLines(linesSoFar)
          } else if (isStats(tokens)) {
            awaitStatsOrEnd(linesSoFar :+ Tokens(tokens.map(ChannelBufferBuf.Owned(_))))
            NeedMoreData
          } else {
            throw new ServerError("Invalid reply from STATS command")
          }
        }
      case AwaitingData(valuesSoFar, tokens, bytesNeeded) =>
        decodeData(bytesNeeded, buffer) { data =>
          awaitResponseOrEnd(
            valuesSoFar :+
              TokensWithData(tokens.map(ChannelBufferBuf.Owned(_)), ChannelBufferBuf.Owned(data))
          )
          NeedMoreData
        }
      case AwaitingResponseOrEnd(valuesSoFar) =>
        decodeLine(buffer, needsData) { tokens =>
          if (isEnd(tokens)) {
            ValueLines(valuesSoFar)
          } else NeedMoreData
        }
    }
  }

  final protected[memcached] def awaitData(tokens: Seq[ChannelBuffer], bytesNeeded: Int): Unit = {
    state match {
      case AwaitingResponse =>
        awaitData(Nil, tokens, bytesNeeded)
      case AwaitingResponseOrEnd(valuesSoFar) =>
        awaitData(valuesSoFar, tokens, bytesNeeded)
    }
  }

  private[this] def awaitData(
    valuesSoFar: Seq[TokensWithData],
    tokens: Seq[ChannelBuffer],
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

  private[this] def isEnd(tokens: Seq[ChannelBuffer]) =
    tokens.length == 1 && tokens.head == END

  private[this] def isStats(tokens: Seq[ChannelBuffer]) =
    tokens.nonEmpty && (tokens.head == STAT || tokens.head == ITEM)

  private[this] val needsData: Seq[ChannelBuffer] => Int = { tokens =>
    val responseName = tokens.head
    if (responseName == VALUE) {
      validateValueResponse(tokens)
      tokens(3).toInt
    } else -1
  }

  private[this] def validateValueResponse(args: Seq[ChannelBuffer]) {
    if (args.length < 4) throw new ServerError("Too few arguments")
    if (args.length > 5) throw new ServerError("Too many arguments")
    if (args.length == 5 && !ParserUtils.isDigits(args(4))) throw new ServerError("CAS must be a number")
    if (!ParserUtils.isDigits(args(3))) throw new ServerError("Bytes must be number")
  }
}
