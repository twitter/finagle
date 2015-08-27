package com.twitter.finagle.memcached.protocol

import com.twitter.io.Buf

sealed abstract class Response
case class NotFound()                     extends Response
case class Stored()                       extends Response
case class NotStored()                    extends Response
case class Exists()                       extends Response
case class Deleted()                      extends Response
case class Error(cause: Exception)        extends Response
case class NoOp()                         extends Response

case class Info(key: Buf, values: Seq[Buf]) extends Response
case class InfoLines(lines: Seq[Info]) extends Response

case class Values(values: Seq[Value])     extends Response
case class Number(value: Long)            extends Response

case class Value(
    key: Buf,
    value: Buf,
    casUnique: Option[Buf] = None,
    flags: Option[Buf] = None)

private[protocol] object Response {
  val NotFound = new NotFound()
  val Stored = new Stored()
  val NotStored = new NotStored()
  val Exists = new Exists()
  val Deleted = new Deleted()
  val NoOp = new NoOp()
}
