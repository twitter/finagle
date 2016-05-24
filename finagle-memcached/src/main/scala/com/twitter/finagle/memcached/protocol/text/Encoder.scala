package com.twitter.finagle.memcached.protocol.text

import com.twitter.finagle.Failure
import com.twitter.finagle.netty3.BufChannelBuffer
import com.twitter.finagle.util.BufWriter
import com.twitter.io.Buf
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder

object Encoder {
  private val SPACE         = " ".getBytes
  private val DELIMITER     = "\r\n".getBytes
  private val END           = "END".getBytes
}

class Encoder extends OneToOneEncoder {
  import Encoder._

  private[this] def encodeTokensWithData(bw: BufWriter, twd: TokensWithData): Unit = twd match {
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

  private[this] def encodeTokens(bw: BufWriter, t: Tokens): Unit = t match {
    case Tokens(tokens) =>
      tokens.foreach { token =>
        bw.writeBytes(Buf.ByteArray.Owned.extract(token))
        bw.writeBytes(SPACE)
      }

      bw.writeBytes(DELIMITER)
  }

  def encode(context: ChannelHandlerContext, channel: Channel, message: Object): Buf = message match {
    case t@Tokens(tokens) =>
      // + 2 to estimated size for DELIMITER.
      val bw = BufWriter.dynamic(10 * tokens.size + 2)
      encodeTokens(bw, t)
      bw.owned()

    case twd@TokensWithData(tokens, data, _) =>
      val bw = BufWriter.dynamic(50 + data.length + 10 * tokens.size)
      encodeTokensWithData(bw, twd)
      bw.owned()

    case ValueLines(lines) =>
      // + 5 to estimated size for END + DELIMITER.
      val bw = BufWriter.dynamic(100 * lines.size + 5)
      lines.foreach { case twd:TokensWithData =>
        encodeTokensWithData(bw, twd)
      }
      bw.writeBytes(END)
      bw.writeBytes(DELIMITER)
      bw.owned()

    case StatLines(lines) =>
      // + 5 to estimated size for END + DELIMITER.
      val bw = BufWriter.dynamic(100 * lines.size + 5)
      lines.foreach { case t: Tokens =>
        encodeTokens(bw, t)
      }
      bw.writeBytes(END)
      bw.writeBytes(DELIMITER)
      bw.owned()
  }
}

private[finagle] class BufToChannelBuf extends SimpleChannelDownstreamHandler {
  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent): Unit =
    e.getMessage match {
      case b: Buf => Channels.write(ctx, e.getFuture, BufChannelBuffer(b))
      case typ => e.getFuture.setFailure(Failure(
        s"unexpected type ${typ.getClass.getSimpleName} when encoding to ChannelBuffer"))
    }
}