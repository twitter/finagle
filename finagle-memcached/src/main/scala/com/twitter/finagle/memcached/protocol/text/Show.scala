package com.twitter.finagle.memcached.protocol.text

import com.twitter.finagle.memcached.protocol._
import com.twitter.io.{Buf, BufByteWriter}
import java.nio.charset.StandardCharsets

private object Encoder {
  val SPACE = " ".getBytes(StandardCharsets.UTF_8)
  val DELIMITER = "\r\n".getBytes(StandardCharsets.UTF_8)
  val END = "END".getBytes(StandardCharsets.UTF_8)
}

/**
 * Class that can encode `Command`-type objects into `Buf`s. Used on the client side.
 */
private[finagle] abstract class AbstractCommandToBuf[Cmd] {
  import Encoder._

  protected final def encodeCommandWithData(
    command: Buf,
    key: Buf,
    flags: Buf,
    expiry: Buf,
    data: Buf,
    casUnique: Option[Buf] = None
  ): Buf = {

    val lengthString = Integer.toString(data.length)

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
        lengthString.length + // trailing space accounted for in casLength, if it's necessary
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

    bw.writeString(lengthString, StandardCharsets.US_ASCII)

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

/**
 * Used by the client.
 */
private[finagle] class CommandToBuf extends AbstractCommandToBuf[Command] {

  private[this] val GET = Buf.Utf8("get")
  private[this] val GETS = Buf.Utf8("gets")
  private[this] val DELETE = Buf.Utf8("delete")
  private[this] val INCR = Buf.Utf8("incr")
  private[this] val DECR = Buf.Utf8("decr")

  private[this] val ADD = Buf.Utf8("add")
  private[this] val SET = Buf.Utf8("set")
  private[this] val APPEND = Buf.Utf8("append")
  private[this] val PREPEND = Buf.Utf8("prepend")
  private[this] val REPLACE = Buf.Utf8("replace")
  private[this] val CAS = Buf.Utf8("cas")

  private[this] val GETV = Buf.Utf8("getv")
  private[this] val UPSERT = Buf.Utf8("upsert")

  private[this] val QUIT = Buf.Utf8("quit")
  private[this] val STATS = Buf.Utf8("stats")

  private[this] val ZeroBuf = Buf.Utf8("0")

  private[this] def intToUtf8(i: Int): Buf =
    if (i == 0) ZeroBuf else Buf.Utf8(i.toString)

  def encode(message: Command): Buf = message match {
    case Add(key, flags, expiry, value) =>
      encodeCommandWithData(ADD, key, intToUtf8(flags), intToUtf8(expiry.inSeconds), value)
    case Set(key, flags, expiry, value) =>
      encodeCommandWithData(SET, key, intToUtf8(flags), intToUtf8(expiry.inSeconds), value)
    case Replace(key, flags, expiry, value) =>
      encodeCommandWithData(REPLACE, key, intToUtf8(flags), intToUtf8(expiry.inSeconds), value)
    case Append(key, flags, expiry, value) =>
      encodeCommandWithData(APPEND, key, intToUtf8(flags), intToUtf8(expiry.inSeconds), value)
    case Prepend(key, flags, expiry, value) =>
      encodeCommandWithData(PREPEND, key, intToUtf8(flags), intToUtf8(expiry.inSeconds), value)
    case Cas(key, flags, expiry, value, casUnique) =>
      encodeCommandWithData(
        CAS,
        key,
        intToUtf8(flags),
        intToUtf8(expiry.inSeconds),
        value,
        Some(casUnique)
      )
    case Upsert(key, flags, expiry, value, version) =>
      encodeCommandWithData(
        UPSERT,
        key,
        intToUtf8(flags),
        intToUtf8(expiry.inSeconds),
        value,
        Some(version)
      )
    case Get(keys) =>
      encodeCommand(GET +: keys)
    case Gets(keys) =>
      encodeCommand(GETS +: keys)
    case Getv(keys) =>
      encodeCommand(GETV +: keys)
    case Incr(key, amount) =>
      encodeCommand(Seq(INCR, key, Buf.Utf8(amount.toString)))
    case Decr(key, amount) =>
      encodeCommand(Seq(DECR, key, Buf.Utf8(amount.toString)))
    case Delete(key) =>
      encodeCommand(Seq(DELETE, key))
    case Stats(args) =>
      encodeCommand(STATS +: args)
    case Quit() =>
      encodeCommand(Seq(QUIT))
  }
}
