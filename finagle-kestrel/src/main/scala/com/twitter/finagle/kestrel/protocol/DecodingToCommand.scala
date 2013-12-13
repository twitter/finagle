package com.twitter.finagle.kestrel.protocol

import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.memcached.protocol.{ClientError, NonexistentCommand}
import org.jboss.netty.buffer.ChannelBuffers.copiedBuffer
import java.nio.charset.Charset
import com.twitter.conversions.time._
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.memcached.protocol.text.server.AbstractDecodingToCommand
import com.twitter.util.Duration

private[kestrel] class DecodingToCommand extends AbstractDecodingToCommand[Command] {
  private[this] val GET         = copiedBuffer("get"        .getBytes)
  private[this] val SET         = copiedBuffer("set"        .getBytes)
  private[this] val DELETE      = copiedBuffer("delete"     .getBytes)
  private[this] val FLUSH       = copiedBuffer("flush"      .getBytes)
  private[this] val FLUSH_ALL   = copiedBuffer("flush_all"  .getBytes)
  private[this] val VERSION     = copiedBuffer("version"    .getBytes)
  private[this] val SHUTDOWN    = copiedBuffer("shutdown"   .getBytes)
  private[this] val STATS       = copiedBuffer("stats"      .getBytes)
  private[this] val DUMP_STATS  = copiedBuffer("dump_stats" .getBytes)

  private[this] val OPEN        = copiedBuffer("open"       .getBytes)
  private[this] val CLOSE       = copiedBuffer("close"      .getBytes)
  private[this] val ABORT       = copiedBuffer("abort"      .getBytes)
  private[this] val PEEK        = copiedBuffer("peek"       .getBytes)

  protected val storageCommands = collection.Set(SET)

  def parseStorageCommand(tokens: Seq[ChannelBuffer], data: ChannelBuffer) = {
    val commandName = tokens.head
    val args = tokens.tail
    commandName match {
      case SET       =>
        val (name, _, expiry, _) = validateStorageCommand(args, data)
        Set(name, expiry, data)
      case _         => throw new NonexistentCommand(commandName.toString)
    }
  }

  def parseNonStorageCommand(tokens: Seq[ChannelBuffer]) = {
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

  private[this] def validateGetCommand(tokens: Seq[ChannelBuffer]): GetCommand = {
    if (tokens.size < 1) throw new ClientError("Key missing")
    if (tokens.size > 1) throw new ClientError("Too many arguments")

    val splitAll = tokens.head.split("/")
    val split = splitAll.filterNot({
      value => value.toString(Charset.defaultCharset).startsWith("t=")
    })
    val queueName = split.head

    val splitTimeout = splitAll.filter({
      value => value.toString(Charset.defaultCharset).startsWith("t=")
    })
    val timeout = splitTimeout.lastOption map {
      t => t.toString(Charset.defaultCharset).drop(2).toInt.milliseconds
    }

    split.tail match {
      case Seq()           => Get(queueName, timeout)
      case Seq(OPEN)       => Open(queueName, timeout)
      case Seq(CLOSE)      => Close(queueName, timeout)
      case Seq(CLOSE,OPEN) => CloseAndOpen(queueName, timeout)
      case Seq(ABORT)      => Abort(queueName, timeout)
      case Seq(PEEK)       => Peek(queueName, timeout)
      case _               => throw new NonexistentCommand(tokens.toString)
    }
  }
}
