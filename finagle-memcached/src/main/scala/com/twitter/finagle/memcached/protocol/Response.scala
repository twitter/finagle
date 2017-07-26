package com.twitter.finagle.memcached.protocol

import com.twitter.io.Buf

sealed abstract class Response
case object NotFound extends Response
case object Stored extends Response
case object NotStored extends Response
case object Exists extends Response
case object Deleted extends Response
case class Error(cause: Exception) extends Response
case object NoOp extends Response

case class Info(key: Buf, values: Seq[Buf])
case class InfoLines(lines: Seq[Info]) extends Response

case class Values(values: Seq[Value]) extends Response
case class Number(value: Long) extends Response

case class Value(key: Buf, value: Buf, casUnique: Option[Buf] = None, flags: Option[Buf] = None)
