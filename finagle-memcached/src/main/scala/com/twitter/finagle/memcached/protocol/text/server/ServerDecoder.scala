package com.twitter.finagle.memcached.protocol.text.server

import com.twitter.finagle.memcached.protocol.ClientError
import com.twitter.finagle.memcached.protocol.text._
import com.twitter.finagle.memcached.util.ParserUtils
import com.twitter.io.Buf
import scala.collection.immutable

private[memcached] object ServerDecoder {

  private val NeedMoreData: Null = null

  private def validateStorageCommand(tokens: Seq[Buf]): Unit = {
    if (tokens.size < 5) throw new ClientError("Too few arguments")
    if (tokens.size > 6) throw new ClientError("Too many arguments")
    if (!ParserUtils.isDigits(tokens(4))) throw new ClientError("Bad frame length")
  }
}

/**
 * Decodes Buf-encoded Commands into Commands. Used by the server.
 *
 * @note Class contains mutable state. Not thread-safe.
 */
private[finagle] abstract class ServerDecoder[R >: Null](storageCommands: immutable.Set[Buf])
    extends Decoder[R] {
  import ServerDecoder._

  /** Type that represents a complete cache value */
  protected type Value

  private sealed trait State
  private case object AwaitingCommand extends State
  private case class AwaitingData(valuesSoFar: Seq[Value], tokens: Seq[Buf], bytesNeeded: Int)
      extends State

  private[this] var state: State = AwaitingCommand

  private[this] def needsData(tokens: Seq[Buf]): Int = {
    val commandName = tokens.head
    if (storageCommands.contains(commandName)) {
      validateStorageCommand(tokens)
      val dataLengthAsBuf = tokens(4)
      ParserUtils.bufToInt(dataLengthAsBuf)
    } else -1
  }

  /** Parse a sequence of tokens into a command */
  protected def parseNonStorageCommand(tokens: Seq[Buf]): R

  protected def parseStorageCommand(tokens: Seq[Buf], data: Buf, casUnique: Option[Buf] = None): R

  def decode(buffer: Buf): R =
    state match {
      case AwaitingCommand =>
        val tokens = ParserUtils.splitOnWhitespace(buffer)
        val dataBytes = needsData(tokens)
        if (dataBytes == -1) {
          parseNonStorageCommand(tokens)
        } else {
          // We are waiting for data next
          state = AwaitingData(Nil, tokens, dataBytes)
          NeedMoreData
        }
      case AwaitingData(_, tokens, bytesNeeded) =>
        // The framer should have given us the right sized Buf
        assert(buffer.length == bytesNeeded)
        state = AwaitingCommand

        val commandName = tokens.head
        if (commandName.equals(Buf.Utf8("cas"))) // cas command
          parseStorageCommand(tokens.slice(0, 5), buffer, Some(tokens(5)))
        else // other commands
          parseStorageCommand(tokens, buffer, None)
    }
}
