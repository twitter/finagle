package com.twitter.finagle.memcached.protocol.text.server

import com.twitter.finagle.memcached.protocol.ClientError
import com.twitter.finagle.memcached.protocol.text._
import com.twitter.finagle.memcached.util.ParserUtils
import com.twitter.io.Buf

private[memcached] object ServerDecoder {

  private sealed trait State
  private case object AwaitingCommand extends State
  private case class AwaitingData(tokens: Seq[Buf], bytesNeeded: Int) extends State

  // Constant for the length of a byte array that will contain a String representation of an Int,
  // which is used in the Decoder class when converting a Buf to an Int
  private val MaxLengthOfIntString = Int.MinValue.toString.length
}

/**
 * Decodes Buf-encoded Commands into Decodings. Used by the server.
 *
 * @note Class contains mutable state. Not thread-safe.
 */
private[finagle] class ServerDecoder(storageCommands: collection.Set[Buf]) extends Decoder {
  import ServerDecoder._

  private[this] var state: State = AwaitingCommand

  def decode(buffer: Buf): Decoding =
    state match {
      case AwaitingCommand =>
        decodeLine(buffer, needsData, awaitData) { tokens =>
          Tokens(tokens)
        }
      case AwaitingData(tokens, bytesNeeded) =>
        decodeData(bytesNeeded, buffer) { data =>
          state = AwaitingCommand

          val commandName = tokens.head
          if (commandName.equals(Buf.Utf8("cas"))) // cas command
            TokensWithData(tokens.slice(0, 5), data, Some(tokens(5)))
          else // other commands
            TokensWithData(tokens, data)
        }
    }

  private[this] def awaitData(tokens: Seq[Buf], bytesNeeded: Int) {
    state = AwaitingData(tokens, bytesNeeded)
  }

  private[this] def needsData(tokens: Seq[Buf]): Int = {
    val commandName = tokens.head
    if (storageCommands.contains(commandName)) {
      validateStorageCommand(tokens)
      val dataLengthAsBuf = tokens(4)
      dataLengthAsBuf.write(byteArrayForBuf2Int, 0)
      ParserUtils.byteArrayStringToInt(byteArrayForBuf2Int, dataLengthAsBuf.length)
    } else -1
  }

  private[this] def validateStorageCommand(tokens: Seq[Buf]) = {
    if (tokens.size < 5) throw new ClientError("Too few arguments")
    if (tokens.size > 6) throw new ClientError("Too many arguments")
    if (!ParserUtils.isDigits(tokens(4))) throw new ClientError("Bad frame length")
  }
}

