package com.twitter.finagle.memcached.protocol.text.server

import scala.Function.tupled
import com.twitter.finagle.memcached.protocol._
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers.{copiedBuffer, hexDump}
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.memcached.util.ParserUtils
import com.twitter.conversions.time._
import com.twitter.util.Time
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import text.{TokensWithData, Tokens}

object DecodingToCommand {
  private val NOREPLY = copiedBuffer("noreply".getBytes)
  private val SET     = copiedBuffer("set"    .getBytes)
  private val ADD     = copiedBuffer("add"    .getBytes)
  private val REPLACE = copiedBuffer("replace".getBytes)
  private val APPEND  = copiedBuffer("append" .getBytes)
  private val PREPEND = copiedBuffer("prepend".getBytes)
  private val GET     = copiedBuffer("get"    .getBytes)
  private val GETS    = copiedBuffer("gets"   .getBytes)
  private val DELETE  = copiedBuffer("delete" .getBytes)
  private val INCR    = copiedBuffer("incr"   .getBytes)
  private val DECR    = copiedBuffer("decr"   .getBytes)
  private val QUIT    = copiedBuffer("quit"   .getBytes)
  private val STATS   = copiedBuffer("stats"  .getBytes)
}

abstract class AbstractDecodingToCommand[C <: AnyRef] extends OneToOneDecoder {
  import ParserUtils._

  // Taken from memcached.c
  private val RealtimeMaxdelta = 60*60*24*30

  def decode(ctx: ChannelHandlerContext, ch: Channel, m: AnyRef) = m match {
    case Tokens(tokens) => parseNonStorageCommand(tokens)
    case TokensWithData(tokens, data, _/*ignore CAS*/) => parseStorageCommand(tokens, data)
  }

  protected def parseNonStorageCommand(tokens: Seq[ChannelBuffer]): C
  protected def parseStorageCommand(tokens: Seq[ChannelBuffer], data: ChannelBuffer): C

  protected def validateStorageCommand(tokens: Seq[ChannelBuffer], data: ChannelBuffer) = {
    val expiry = tokens(2).toInt match {
      case 0 => 0.seconds.afterEpoch
      case unixtime if unixtime > RealtimeMaxdelta => Time.fromSeconds(unixtime)
      case delta => delta.seconds.fromNow
    }
    (tokens(0), tokens(1).toInt, expiry, data)
  }

  protected def validateDeleteCommand(tokens: Seq[ChannelBuffer]) = {
    if (tokens.size < 1) throw new ClientError("No key")
    if (tokens.size == 2 && !tokens.last.matches(DIGITS)) throw new ClientError("Timestamp is poorly formed")
    if (tokens.size > 2) throw new ClientError("Too many arguments")

    tokens.head
  }
}

class DecodingToCommand extends AbstractDecodingToCommand[Command] {
  import DecodingToCommand._
  import ParserUtils._

  private[this] def validateArithmeticCommand(tokens: Seq[ChannelBuffer]) = {
    if (tokens.size < 2) throw new ClientError("Too few arguments")
    if (tokens.size == 3 && tokens.last != NOREPLY) throw new ClientError("Too many arguments")
    if (!tokens(1).matches(DIGITS)) throw new ClientError("Delta is not a number")

    (tokens.head, tokens(1).toLong)
  }

  private[this] def validateAnyStorageCommand(tokens: Seq[ChannelBuffer]) {
    if (tokens.isEmpty) throw new ClientError("No arguments specified")
  }

  protected def parseStorageCommand(tokens: Seq[ChannelBuffer], data: ChannelBuffer) = {
    validateAnyStorageCommand(tokens)
    val commandName = tokens.head
    val args = tokens.tail
    commandName match {
      case SET       => tupled(Set)(validateStorageCommand(args, data))
      case ADD       => tupled(Add)(validateStorageCommand(args, data))
      case REPLACE   => tupled(Replace)(validateStorageCommand(args, data))
      case APPEND    => tupled(Append)(validateStorageCommand(args, data))
      case PREPEND   => tupled(Prepend)(validateStorageCommand(args, data))
      case _         => throw new NonexistentCommand(hexDump(commandName))
    }
  }

  protected def parseNonStorageCommand(tokens: Seq[ChannelBuffer]) = {
    validateAnyStorageCommand(tokens)
    val commandName = tokens.head
    val args = tokens.tail
    commandName match {
      case GET     => Get(args)
      case GETS    => Get(args)
      case DELETE  => Delete(validateDeleteCommand(args))
      case INCR    => tupled(Incr)(validateArithmeticCommand(args))
      case DECR    => tupled(Decr)(validateArithmeticCommand(args))
      case QUIT    => Quit()
      case STATS   => Stats(args)
      case _       => throw new NonexistentCommand(hexDump(commandName))
    }
  }

}
