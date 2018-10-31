package com.twitter.finagle.memcached.protocol.text.client

import com.twitter.finagle.memcached.protocol._
import com.twitter.io.Buf

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
