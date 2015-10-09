package com.twitter.finagle.kestrel.protocol

import com.twitter.finagle.memcached.protocol.{ClientError, NonexistentCommand}
import com.twitter.conversions.time._
import com.twitter.finagle.memcached.protocol.text.server.AbstractDecodingToCommand
import com.twitter.finagle.memcached.util.Bufs.RichBuf
import com.twitter.io.Buf

private[kestrel] class DecodingToCommand extends AbstractDecodingToCommand[Command] {
  private[this] val TimestampPrefix = Buf.Utf8("t=")

  private[this] val GET         = Buf.Utf8("get")
  private[this] val SET         = Buf.Utf8("set")
  private[this] val DELETE      = Buf.Utf8("delete")
  private[this] val FLUSH       = Buf.Utf8("flush")
  private[this] val FLUSH_ALL   = Buf.Utf8("flush_all")
  private[this] val VERSION     = Buf.Utf8("version")
  private[this] val SHUTDOWN    = Buf.Utf8("shutdown")
  private[this] val STATS       = Buf.Utf8("stats")
  private[this] val DUMP_STATS  = Buf.Utf8("dump_stats")

  private[this] val OPEN        = Buf.Utf8("open")
  private[this] val CLOSE       = Buf.Utf8("close")
  private[this] val ABORT       = Buf.Utf8("abort")
  private[this] val PEEK        = Buf.Utf8("peek")

  protected val storageCommands = collection.Set(SET)

  def parseStorageCommand(tokens: Seq[Buf], data: Buf) = {
    val commandName = tokens.head
    val args = tokens.tail
    commandName match {
      case SET       =>
        val (name, _, expiry, _) = validateStorageCommand(args, data)
        Set(name, expiry, data)
      case _         => throw new NonexistentCommand(commandName.toString)
    }
  }

  def parseNonStorageCommand(tokens: Seq[Buf]) = {
    val commandName = tokens.head
    val args = tokens.tail
    commandName match {
      case GET        => validateGetCommand(args)
      case DELETE     => Delete(validateDeleteCommand(args))
      case FLUSH      => Flush(validateDeleteCommand(args))
      case FLUSH_ALL  => FlushAll()
      case VERSION    => Version()
      case SHUTDOWN   => ShutDown()
      case STATS      => Stats()
      case DUMP_STATS => DumpStats()
      case _          => throw new NonexistentCommand(commandName.toString)
    }
  }

  private[this] def validateGetCommand(tokens: Seq[Buf]): GetCommand = {
    if (tokens.size < 1) throw new ClientError("Key missing")
    if (tokens.size > 1) throw new ClientError("Too many arguments")

    val splitAll = tokens.head.split('/')

    val (splitTimeout, split) = splitAll partition { (value: Buf) => value.startsWith(TimestampPrefix) }

    val queueName = split.head

    val timeout = splitTimeout.lastOption.map {
      case Buf.Utf8(s) => s.drop(2).toInt.milliseconds
    }

    split.tail match {
      case Seq()           => Get(queueName, timeout)
      case Seq(OPEN)       => Open(queueName, timeout)
      case Seq(CLOSE)      => Close(queueName, timeout)
      case Seq(CLOSE,OPEN) => CloseAndOpen(queueName, timeout)
      case Seq(ABORT)      => Abort(queueName, timeout)
      case Seq(PEEK)       => Peek(queueName, timeout)
      case _               => throw new NonexistentCommand(tokens.map { case Buf.Utf8(s) => s }.mkString)
    }
  }
}
