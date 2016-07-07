package com.twitter.finagle.memcached.protocol.text.client

import com.twitter.finagle.memcached.protocol.text.Framer
import com.twitter.finagle.memcached.util.ParserUtils
import com.twitter.io.Buf

private object ClientFramer {
  val Value = Buf.Utf8("VALUE")
}

private[finagle] class ClientFramer extends Framer {
  import ClientFramer._

  // The data length is the 4th token, interpreted as an Int.
  def dataLength(tokens: IndexedSeq[Buf]): Int =
    if (tokens.nonEmpty) {
      val responseName = tokens.head
      if (responseName == Value && tokens.length >= 4) {
        val dataLengthAsBuf = tokens(3)
        dataLengthAsBuf.write(byteArrayForBuf2Int, 0)
        ParserUtils.byteArrayStringToInt(byteArrayForBuf2Int, dataLengthAsBuf.length)
      } else -1
    } else -1
}
