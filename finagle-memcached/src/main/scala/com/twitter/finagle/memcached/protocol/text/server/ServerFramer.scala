package com.twitter.finagle.memcached.protocol.text.server

import com.twitter.finagle.memcached.protocol.text.Framer
import com.twitter.finagle.memcached.util.ParserUtils
import com.twitter.io.Buf

private[finagle] class ServerFramer(storageCommands: Set[Buf]) extends Framer {

  // The data length is the 5th token, interpreted as an Int.
  def dataLength(tokens: IndexedSeq[Buf]): Int =
    if (tokens.nonEmpty) {
      val commandName = tokens.head
      if (storageCommands.contains(commandName) && tokens.length >= 5) {
        val dataLengthAsBuf = tokens(4)
        dataLengthAsBuf.write(byteArrayForBuf2Int, 0)
        ParserUtils.byteArrayStringToInt(byteArrayForBuf2Int, dataLengthAsBuf.length)
      } else -1
    } else -1
}
