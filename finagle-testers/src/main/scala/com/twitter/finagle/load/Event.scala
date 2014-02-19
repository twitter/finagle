package com.twitter.finagle.load

import com.twitter.util.{Duration, Time, Try}

case class Event[Req, Rep](start: Time, length: Duration, arg: Req, fn: Req => Try[Rep]) {
  def apply(): Try[Rep] = fn(arg)
  val finish: Time = start + length
}
