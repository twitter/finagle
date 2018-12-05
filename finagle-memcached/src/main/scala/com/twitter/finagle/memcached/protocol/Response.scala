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

// 'ValuesAndErrors' is a special Response type used on the client side to handle partial success
// for batched operations. When the keys belong to multiple partitions some partitions can fail to
// serve the request. The successful responses are collected in 'values' sequence and
// all errors are collected in the errors map against the requested key. This allows the caller
// to retry only the failed keys instead of the entire batch.
case class ValuesAndErrors(
  values: Seq[Value], // successful results
  errors: Map[Buf, Throwable]) // keys that error'ed out (eligible for retries)
    extends Response

case class Number(value: Long) extends Response

case class Value(key: Buf, value: Buf, casUnique: Option[Buf] = None, flags: Option[Buf] = None)
