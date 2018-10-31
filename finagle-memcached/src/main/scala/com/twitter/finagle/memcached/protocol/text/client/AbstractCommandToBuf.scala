package com.twitter.finagle.memcached.protocol.text.client

import com.twitter.finagle.memcached.protocol.text.EncodingConstants._
import com.twitter.finagle.memcached.protocol.text.MessageEncoder
import com.twitter.io.{Buf, BufByteWriter}

private[finagle] object AbstractCommandToBuf {
  def lengthAsString(x: Int): Int = {
    if (x < 0) throw new IllegalArgumentException(s"x must be non-negative: $x")
    var c: Int = 9
    var size: Int = 1
    while (x > c) {
      size += 1
      c = 10 * c + 9
    }
    size
  }

  private[this] val ascii0: Int = '0'.toInt

  def writeDigits(x: Int, bw: BufByteWriter): Unit = {
    if (x < 0) throw new IllegalArgumentException(s"x must be non-negative: $x")
    if (10 <= x) {
      writeDigits(x / 10, bw)
    }
    bw.writeByte(ascii0 + (x % 10))
  }
}

/**
 * Class that can encode `Command`-type objects into `Buf`s. Used on the client side.
 */
private[finagle] abstract class AbstractCommandToBuf[Cmd] extends MessageEncoder[Cmd] {
  import AbstractCommandToBuf._

  protected final def encodeCommandWithData(
    command: Buf,
    key: Buf,
    flags: Buf,
    expiry: Buf,
    data: Buf,
    casUnique: Option[Buf] = None
  ): Buf = {
    val dataLength = data.length

    val messageSize = {
      val casLength = casUnique match {
        case Some(token) => 1 + token.length // SPACE + token
        case None => 0
      }

      // the '+ 1' accounts for the space separator
      command.length + 1 +
        key.length + 1 +
        flags.length + 1 +
        expiry.length + 1 +
        lengthAsString(dataLength) + // trailing space accounted for in casLength, if it's necessary
        casLength + 2 + // CAS + '\r\n'
        data.length + 2 // data + '\r\n'
    }

    val bw = BufByteWriter.fixed(messageSize)

    bw.writeBytes(command)
    bw.writeBytes(SPACE)

    bw.writeBytes(key)
    bw.writeBytes(SPACE)

    bw.writeBytes(flags)
    bw.writeBytes(SPACE)

    bw.writeBytes(expiry)
    bw.writeBytes(SPACE)

    writeDigits(dataLength, bw)

    casUnique match {
      case Some(token) =>
        bw.writeBytes(SPACE)
        bw.writeBytes(token)
      case None => ()
    }

    bw.writeBytes(DELIMITER)
    bw.writeBytes(data)
    bw.writeBytes(DELIMITER)

    bw.owned()
  }

  protected final def encodeCommand(command: Seq[Buf]): Buf = {
    // estimated size + 2 for DELIMITER
    val bw = BufByteWriter.dynamic(10 * command.size + 2)
    command.foreach { token =>
      bw.writeBytes(token)
      bw.writeBytes(SPACE)
    }
    bw.writeBytes(DELIMITER)
    bw.owned()
  }

  def encode(message: Cmd): Buf
}
