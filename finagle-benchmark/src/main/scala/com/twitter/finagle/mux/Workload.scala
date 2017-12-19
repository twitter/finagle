package com.twitter.finagle.mux

import com.twitter.finagle.{Dtab, Path}
import com.twitter.finagle.mux.transport.Message
import com.twitter.io.Buf
import com.twitter.util.Time

object Workload {
  val RreqOk = Message.RreqOk(0, Buf.Utf8("foo"))
  val RreqError = Message.RreqError(0, "bad news")
  val RreqNack = Message.RreqNack(0)
  val Tdispatch = Message.Tdispatch(0, Seq.empty, Path.empty, Dtab.empty, Buf.Utf8("foo"))
  val RdispatchOk = Message.RdispatchOk(0, Seq.empty, Buf.Utf8("foo"))
  val RdispatchError = Message.RdispatchError(0, Seq.empty, "bad")
  val RdispatchNack = Message.RdispatchNack(0, Seq.empty)
  val Tdrain = Message.Tdrain(0)
  val Rdrain = Message.Rdrain(0)
  val Tping = Message.Tping(0)
  val Rping = Message.Rping(0)
  val Tdiscarded = Message.Tdiscarded(0, "give up already")
  val Rdiscarded = Message.Rdiscarded(0)
  val Tlease = Message.Tlease(Time.now)
  val Tinit = Message.Tinit(0, 0, Seq.empty)
  val Rinit = Message.Rinit(0, 0, Seq.empty)
  val Rerr = Message.Rerr(0, "bad news")
  val TReq = Message.Treq(0, None, Buf.Utf8("foo"))
}
