package com.twitter.finagle.memcached.protocol.text

import com.twitter.io.{Buf, ByteWriter}

object Encoder {
  private val SPACE         = " ".getBytes
  private val DELIMITER     = "\r\n".getBytes
  private val END           = "END".getBytes
}

class Encoder {
  import Encoder._

  private[this] def encodeTokensWithData(bw: ByteWriter, twd: TokensWithData): Unit = twd match {
    case TokensWithData(tokens, data, casUnique) =>
      tokens.foreach { token =>
        bw.writeBytes(Buf.ByteArray.Owned.extract(token))
        bw.writeBytes(SPACE)
      }

      bw.writeBytes(Buf.ByteArray.Owned.extract(Buf.Utf8(data.length.toString)))

      casUnique.foreach { token =>
        bw.writeBytes(SPACE)
        bw.writeBytes(Buf.ByteArray.Owned.extract(token))
      }

      bw.writeBytes(DELIMITER)
      bw.writeBytes(Buf.ByteArray.Owned.extract(data))
      bw.writeBytes(DELIMITER)
  }

  private[this] def encodeTokens(bw: ByteWriter, t: Tokens): Unit = t match {
    case Tokens(tokens) =>
      tokens.foreach { token =>
        bw.writeBytes(Buf.ByteArray.Owned.extract(token))
        bw.writeBytes(SPACE)
      }

      bw.writeBytes(DELIMITER)
  }

  def encode(message: Decoding): Buf = message match {
    case t@Tokens(tokens) =>
      // + 2 to estimated size for DELIMITER.
      val bw = ByteWriter.dynamic(10 * tokens.size + 2)
      encodeTokens(bw, t)
      bw.owned()

    case twd@TokensWithData(tokens, data, _) =>
      val bw = ByteWriter.dynamic(50 + data.length + 10 * tokens.size)
      encodeTokensWithData(bw, twd)
      bw.owned()

    case ValueLines(lines) =>
      // + 5 to estimated size for END + DELIMITER.
      val bw = ByteWriter.dynamic(100 * lines.size + 5)
      lines.foreach { case twd:TokensWithData =>
        encodeTokensWithData(bw, twd)
      }
      bw.writeBytes(END)
      bw.writeBytes(DELIMITER)
      bw.owned()

    case StatLines(lines) =>
      // + 5 to estimated size for END + DELIMITER.
      val bw = ByteWriter.dynamic(100 * lines.size + 5)
      lines.foreach { case t: Tokens =>
        encodeTokens(bw, t)
      }
      bw.writeBytes(END)
      bw.writeBytes(DELIMITER)
      bw.owned()
  }
}
