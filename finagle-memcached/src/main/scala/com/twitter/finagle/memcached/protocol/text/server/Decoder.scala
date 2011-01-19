package com.twitter.finagle.memcached.protocol.text.server

import org.jboss.netty.channel._
import com.twitter.util.StateMachine
import com.twitter.finagle.memcached.protocol.Command
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.memcached.protocol.text.{Show, AbstractDecoder, ParseCommand}
import org.jboss.netty.util.CharsetUtil

class Decoder extends AbstractDecoder[Command] with StateMachine {
  case class AwaitingCommand() extends State
  case class AwaitingData(tokens: Seq[ChannelBuffer], bytesNeeded: Int) extends State

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    Channels.write(ctx.getChannel, Show(e.getCause))
    super.exceptionCaught(ctx, e)
  }

  def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer): Command = {
    state match {
      case AwaitingCommand() =>
        decodeLine(buffer, ParseCommand.needsData(_)) { tokens =>
          ParseCommand(tokens)
        }
      case AwaitingData(tokens, bytesNeeded) =>
        decodeData(bytesNeeded, buffer) { data =>
          ParseCommand(tokens, data)
        }
    }
  }

  protected def awaitData(tokens: Seq[ChannelBuffer], bytesNeeded: Int) = {
    state = AwaitingData(tokens, bytesNeeded)
    needMoreData
  }

  protected def start() = {
    state = AwaitingCommand()
  }

  protected val needMoreData: Command = null
}