package com.twitter.finagle.memcached.protocol.text

import com.twitter.io.{Buf, ByteWriter}
import java.nio.charset.StandardCharsets

private object Encoder {
  val SPACE = " ".getBytes(StandardCharsets.UTF_8)
  val DELIMITER = "\r\n".getBytes(StandardCharsets.UTF_8)
  val END = "END".getBytes(StandardCharsets.UTF_8)
}

class Encoder {
  import Encoder._

  private[this] def encodeTokensWithData(bw: ByteWriter, twd: TokensWithData): Unit = twd match {
    case TokensWithData(tokens, data, casUnique) =>
      tokens.foreach { token =>
        bw.writeBytes(token)
        bw.writeBytes(SPACE)
      }

      bw.writeBytes(data.length.toString.getBytes(StandardCharsets.US_ASCII))

      casUnique.foreach { token =>
        bw.writeBytes(SPACE)
        bw.writeBytes(token)
      }

      bw.writeBytes(DELIMITER)
      bw.writeBytes(data)
      bw.writeBytes(DELIMITER)
  }

  private[this] def encodeTokens(bw: ByteWriter, t: Tokens): Unit = t match {
    case Tokens(tokens) =>
      tokens.foreach { token =>
        bw.writeBytes(token)
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
