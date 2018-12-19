package com.twitter.finagle.memcached.protocol.text.server

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.memcached.util.Bufs.RichBuf
import com.twitter.finagle.memcached.util.ParserUtils.isDigits
import com.twitter.io.Buf
import com.twitter.util.Time
import scala.collection.immutable
import scala.Function.tupled

object MemcachedServerDecoder {
  private val NOREPLY = Buf.Utf8("noreply")
  private val SET = Buf.Utf8("set")
  private val ADD = Buf.Utf8("add")
  private val REPLACE = Buf.Utf8("replace")
  private val APPEND = Buf.Utf8("append")
  private val PREPEND = Buf.Utf8("prepend")
  private val GET = Buf.Utf8("get")
  private val GETS = Buf.Utf8("gets")
  private val DELETE = Buf.Utf8("delete")
  private val INCR = Buf.Utf8("incr")
  private val DECR = Buf.Utf8("decr")
  private val QUIT = Buf.Utf8("quit")
  private val STATS = Buf.Utf8("stats")
  private val CAS = Buf.Utf8("cas")

  // Taken from memcached.c
  private[this] val RealtimeMaxdelta = 60 * 60 * 24 * 30

  private def validateArithmeticCommand(tokens: Seq[Buf]) = {
    if (tokens.size < 2) throw new ClientError("Too few arguments")
    if (tokens.size == 3 && tokens.last != NOREPLY) throw new ClientError("Too many arguments")
    if (!isDigits(tokens(1))) throw new ClientError("Delta is not a number")

    (tokens.head, tokens(1).toLong)
  }

  protected def validateDeleteCommand(tokens: Seq[Buf]): Buf = {
    if (tokens.size < 1) throw new ClientError("No key")
    if (tokens.size == 2 && !isDigits(tokens.last))
      throw new ClientError("Timestamp is poorly formed")
    if (tokens.size > 2) throw new ClientError("Too many arguments")

    tokens.head
  }

  protected def validateStorageCommand(tokens: Seq[Buf], data: Buf): (Buf, Int, Time, Buf) = {
    val expiry = getExpiry(tokens)
    (tokens(0), tokens(1).toInt, expiry, data)
  }

  protected def validateCasCommand(
    tokens: Seq[Buf],
    data: Buf,
    casUnique: Buf
  ): (Buf, Int, Time, Buf, Buf) = {
    val expiry = getExpiry(tokens)
    (tokens(0), tokens(1).toInt, expiry, data, casUnique)
  }

  private def getExpiry(tokens: Seq[Buf]): Time = {
    tokens(2).toInt match {
      case 0 => Time.epoch
      case unixtime if unixtime > RealtimeMaxdelta => Time.fromSeconds(unixtime)
      case delta => delta.seconds.fromNow
    }
  }
}

private[memcached] class MemcachedServerDecoder(storageCommands: immutable.Set[Buf])
    extends ServerDecoder[Command](storageCommands) {
  import MemcachedServerDecoder._

  private[this] def validateAnyStorageCommand(tokens: Seq[Buf]): Unit = {
    if (tokens.isEmpty) throw new ClientError("No arguments specified")
  }

  protected def parseNonStorageCommand(tokens: Seq[Buf]): Command = {
    validateAnyStorageCommand(tokens)
    val commandName = tokens.head
    val args = tokens.tail
    commandName match {
      case GET => Get(args)
      case GETS => Gets(args)
      case DELETE => Delete(validateDeleteCommand(args))
      case INCR => tupled(Incr)(validateArithmeticCommand(args))
      case DECR => tupled(Decr)(validateArithmeticCommand(args))
      case QUIT => Quit()
      case STATS => Stats(args)
      case _ => throw new NonexistentCommand(Buf.slowHexString(commandName))
    }
  }

  protected def parseStorageCommand(tokens: Seq[Buf], data: Buf, casUnique: Option[Buf]) = {
    validateAnyStorageCommand(tokens)
    val commandName = tokens.head
    val args = tokens.tail
    commandName match {
      case SET => tupled(Set)(validateStorageCommand(args, data))
      case ADD => tupled(Add)(validateStorageCommand(args, data))
      case REPLACE => tupled(Replace)(validateStorageCommand(args, data))
      case APPEND => tupled(Append)(validateStorageCommand(args, data))
      case PREPEND => tupled(Prepend)(validateStorageCommand(args, data))
      case CAS => {
        val casUniqueValue =
          casUnique.getOrElse(throw new ServerError("checksum is missing for a CAS command"))
        tupled(Cas)(validateCasCommand(args, data, casUniqueValue))
      }
      case _ => throw new NonexistentCommand(Buf.slowHexString(commandName))
    }
  }
}
