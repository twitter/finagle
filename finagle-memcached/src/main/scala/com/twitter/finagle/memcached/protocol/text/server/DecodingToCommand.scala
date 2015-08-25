package com.twitter.finagle.memcached.protocol.text.server

import scala.Function.tupled

import org.jboss.netty.handler.codec.oneone.OneToOneDecoder
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}

import com.twitter.conversions.time._
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.memcached.util.Bufs.RichBuf
import com.twitter.finagle.memcached.util.ParserUtils
import com.twitter.io.Buf
import com.twitter.util.Time

import text.{TokensWithData, Tokens}

object DecodingToCommand {
  private val NOREPLY = Buf.Utf8("noreply")
  private val SET     = Buf.Utf8("set")
  private val ADD     = Buf.Utf8("add")
  private val REPLACE = Buf.Utf8("replace")
  private val APPEND  = Buf.Utf8("append")
  private val PREPEND = Buf.Utf8("prepend")
  private val GET     = Buf.Utf8("get")
  private val GETS    = Buf.Utf8("gets")
  private val DELETE  = Buf.Utf8("delete")
  private val INCR    = Buf.Utf8("incr")
  private val DECR    = Buf.Utf8("decr")
  private val QUIT    = Buf.Utf8("quit")
  private val STATS   = Buf.Utf8("stats")
}

abstract class AbstractDecodingToCommand[C <: AnyRef] extends OneToOneDecoder {
  import ParserUtils._

  // Taken from memcached.c
  private val RealtimeMaxdelta = 60*60*24*30

  def decode(ctx: ChannelHandlerContext, ch: Channel, m: AnyRef) = m match {
    case Tokens(tokens) => parseNonStorageCommand(tokens)
    case TokensWithData(tokens, data, _/*ignore CAS*/) => parseStorageCommand(tokens, data)
  }

  protected def parseNonStorageCommand(tokens: Seq[Buf]): C
  protected def parseStorageCommand(tokens: Seq[Buf], data: Buf): C

  protected def validateStorageCommand(tokens: Seq[Buf], data: Buf) = {
    val expiry = tokens(2).toInt match {
      case 0 => 0.seconds.afterEpoch
      case unixtime if unixtime > RealtimeMaxdelta => Time.fromSeconds(unixtime)
      case delta => delta.seconds.fromNow
    }
    (tokens(0), tokens(1).toInt, expiry, data)
  }

  protected def validateDeleteCommand(tokens: Seq[Buf]): Buf = {
    if (tokens.size < 1) throw new ClientError("No key")
    if (tokens.size == 2 && !isDigits(tokens.last)) throw new ClientError("Timestamp is poorly formed")
    if (tokens.size > 2) throw new ClientError("Too many arguments")

    tokens.head
  }
}

class DecodingToCommand extends AbstractDecodingToCommand[Command] {
  import DecodingToCommand._
  import ParserUtils._

  private[this] def validateArithmeticCommand(tokens: Seq[Buf]) = {
    if (tokens.size < 2) throw new ClientError("Too few arguments")
    if (tokens.size == 3 && tokens.last != NOREPLY) throw new ClientError("Too many arguments")
    if (!isDigits(tokens(1))) throw new ClientError("Delta is not a number")

    (tokens.head, tokens(1).toLong)
  }

  private[this] def validateAnyStorageCommand(tokens: Seq[Buf]) {
    if (tokens.isEmpty) throw new ClientError("No arguments specified")
  }

  protected def parseStorageCommand(tokens: Seq[Buf], data: Buf) = {
    validateAnyStorageCommand(tokens)
    val commandName = tokens.head
    val args = tokens.tail
    commandName match {
      case SET       => tupled(Set)(validateStorageCommand(args, data))
      case ADD       => tupled(Add)(validateStorageCommand(args, data))
      case REPLACE   => tupled(Replace)(validateStorageCommand(args, data))
      case APPEND    => tupled(Append)(validateStorageCommand(args, data))
      case PREPEND   => tupled(Prepend)(validateStorageCommand(args, data))
      case _         => throw new NonexistentCommand(Buf.slowHexString(commandName))
    }
  }

  protected def parseNonStorageCommand(tokens: Seq[Buf]) = {
    validateAnyStorageCommand(tokens)
    val commandName = tokens.head
    val args = tokens.tail
    commandName match {
      case GET     => Get(args)
      case GETS    => Gets(args)
      case DELETE  => Delete(validateDeleteCommand(args))
      case INCR    => tupled(Incr)(validateArithmeticCommand(args))
      case DECR    => tupled(Decr)(validateArithmeticCommand(args))
      case QUIT    => Quit()
      case STATS   => Stats(args)
      case _       => throw new NonexistentCommand(Buf.slowHexString(commandName))
    }
  }
}
