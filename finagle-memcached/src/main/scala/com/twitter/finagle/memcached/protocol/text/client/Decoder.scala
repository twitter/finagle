package com.twitter.finagle.memcached.protocol.text.client

import org.jboss.netty.channel._
import com.twitter.util.StateMachine
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.memcached.protocol.ServerError
import com.twitter.finagle.memcached.util.ParserUtils
import com.twitter.finagle.memcached.protocol.text._

object Decoder {
  private val END   = "END": ChannelBuffer
  private val ITEM  = "ITEM": ChannelBuffer
  private val STAT  = "STAT": ChannelBuffer
  private val VALUE = "VALUE": ChannelBuffer
}

class Decoder extends AbstractDecoder with StateMachine {
  import Decoder._
  import ParserUtils._

  case class AwaitingResponse()                                                  extends State
  case class AwaitingResponseOrEnd(valuesSoFar: Seq[TokensWithData])             extends State
  case class AwaitingStatsOrEnd(valuesSoFar: Seq[Tokens])                        extends State
  case class AwaitingData(valuesSoFar: Seq[TokensWithData], tokens: Seq[ChannelBuffer], bytesNeeded: Int) extends State

  final protected[memcached] def start() {
    state = AwaitingResponse()
  }

  def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer): Decoding = {
    state match {
      case AwaitingResponse() =>
        decodeLine(buffer, needsData(_)) { tokens =>
          if (isEnd(tokens)) {
            ValueLines(Seq[TokensWithData]())
          } else if (isStats(tokens)) {
            awaitStatsOrEnd(Seq(Tokens(tokens)))
            needMoreData
          } else {
            Tokens(tokens)
          }
        }
      case AwaitingStatsOrEnd(linesSoFar) =>
        decodeLine(buffer, needsData(_)) { tokens =>
          if (isEnd(tokens)) {
            StatLines(linesSoFar)
          } else if (isStats(tokens)) {
            awaitStatsOrEnd(linesSoFar ++ Seq(Tokens(tokens)))
            needMoreData
          } else {
            throw new ServerError("Invalid reply from STATS command")
          }
        }
      case AwaitingData(valuesSoFar, tokens, bytesNeeded) =>
        decodeData(bytesNeeded, buffer) { data =>
          awaitResponseOrEnd(
            valuesSoFar ++
            Seq(TokensWithData(tokens, data)))
          needMoreData
        }
      case AwaitingResponseOrEnd(valuesSoFar) =>
        decodeLine(buffer, needsData(_)) { tokens =>
          if (isEnd(tokens)) {
            ValueLines(valuesSoFar)
          } else needMoreData
        }
    }
  }

  final protected[memcached] def awaitData(tokens: Seq[ChannelBuffer], bytesNeeded: Int) = {
    state match {
      case AwaitingResponse() =>
        awaitData(Seq(), tokens, bytesNeeded)
      case AwaitingResponseOrEnd(valuesSoFar) =>
        awaitData(valuesSoFar, tokens, bytesNeeded)
    }
  }

  private[this] def awaitData(valuesSoFar: Seq[TokensWithData], tokens: Seq[ChannelBuffer], bytesNeeded: Int) {
    state = AwaitingData(valuesSoFar, tokens, bytesNeeded)
  }

  private[this] def awaitResponseOrEnd(valuesSoFar: Seq[TokensWithData]) {
    state = AwaitingResponseOrEnd(valuesSoFar)
  }

  private[this] def awaitStatsOrEnd(valuesSoFar: Seq[Tokens]) {
    state = AwaitingStatsOrEnd(valuesSoFar)
  }

  private[this] val needMoreData = null: Decoding

  private[this] def isEnd(tokens: Seq[ChannelBuffer]) =
    (tokens.length == 1 && tokens.head == END)

  private[this] def isStats(tokens: Seq[ChannelBuffer]) =
    (tokens.length > 0 && (tokens.head == STAT || tokens.head == ITEM))

  private[this] def needsData(tokens: Seq[ChannelBuffer]) = {
    val responseName = tokens.head
    val args = tokens.tail
    if (responseName == VALUE) {
      validateValueResponse(args)
      Some(args(2).toInt)
    } else None
  }


  private[this] def validateValueResponse(args: Seq[ChannelBuffer]) {
    if (args.length < 3) throw new ServerError("Too few arguments")
    if (args.length > 4) throw new ServerError("Too many arguments")
    if (args.length == 4 && !args(3).matches(DIGITS)) throw new ServerError("CAS must be a number")
    if (!args(2).matches(DIGITS)) throw new ServerError("Bytes must be number")
  }
}
