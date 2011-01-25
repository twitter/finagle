package com.twitter.finagle.memcached.protocol.text.client

import org.jboss.netty.channel._
import com.twitter.util.StateMachine
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.memcached.protocol.Response
import com.twitter.finagle.memcached.protocol.text.{ResponseVocabulary, AbstractDecoder}

case class ValueLine(tokens: Seq[ChannelBuffer], buffer: ChannelBuffer)

object Decoder {
  private val END    = "END": ChannelBuffer
}

class Decoder[R >: Null <: AnyRef](parser: ResponseVocabulary[R]) extends AbstractDecoder[R] with StateMachine {
  import Decoder._
  case class AwaitingResponse()                                             extends State
  case class AwaitingResponseOrEnd(valuesSoFar: Seq[ValueLine])             extends State
  case class AwaitingData(valuesSoFar: Seq[ValueLine], tokens: Seq[ChannelBuffer], bytesNeeded: Int) extends State

  final protected[memcached] def start() {
    state = AwaitingResponse()
  }

  def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer): R = {
    state match {
      case AwaitingResponse() =>
        decodeLine(buffer, parser.needsData(_)) { tokens =>
          if (isEnd(tokens)) {
            parser.parseValues(Seq())
          } else {
            parser.parseResponse(tokens)
          }
        }
      case AwaitingData(valuesSoFar, tokens, bytesNeeded) =>
        decodeData(bytesNeeded, buffer) { data =>
          awaitResponseOrEnd(
            valuesSoFar ++
            Seq(ValueLine(tokens, ChannelBuffers.copiedBuffer(data))))
        }
      case AwaitingResponseOrEnd(valuesSoFar) =>
        decodeLine(buffer, parser.needsData(_)) { tokens =>
          if (isEnd(tokens)) {
            parser.parseValues(valuesSoFar)
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

  private[this] def awaitData(valuesSoFar: Seq[ValueLine], tokens: Seq[ChannelBuffer], bytesNeeded: Int): R = {
    state = AwaitingData(valuesSoFar, tokens, bytesNeeded)
    needMoreData
  }

  private[this] def awaitResponseOrEnd(valuesSoFar: Seq[ValueLine]): R = {
    state = AwaitingResponseOrEnd(valuesSoFar)
    needMoreData
  }

  private[this] val needMoreData = null

  private[this] def isEnd(tokens: Seq[ChannelBuffer]) =
    (tokens.length == 1 && tokens.head == END)
}