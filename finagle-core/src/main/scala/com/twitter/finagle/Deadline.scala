package com.twitter.finagle

import com.twitter.finagle.util.ByteArrays
import com.twitter.finagle.context.Contexts
import com.twitter.io.Buf
import com.twitter.util.{Try, Return, Throw, Time, Duration}

/**
 * A deadline is the time by which some action (e.g., a request) must
 * complete. A deadline has a timestamp in addition to the deadline.
 * This timestamp denotes the time at which the deadline was enacted.
 *
 * This is done so that they may be reconciled over process boundaries;
 * e.g., to account for variable latencies in message deliveries.
 *
 * @param timestamp the time at which the deadline was enacted.
 *
 * @param deadline the time by which the action must complete.
 */
case class Deadline(timestamp: Time, deadline: Time) extends Ordered[Deadline] {
  def compare(that: Deadline): Int = this.deadline.compare(that.deadline)
  def expired: Boolean = Time.now > deadline
  def remaining: Duration = deadline-Time.now
}

/**
 * A broadcast context for deadlines.
 */
object Deadline extends Contexts.broadcast.Key[Deadline]("com.twitter.finagle.Deadline") {
  /**
   * Construct a deadline from a timeout.
   */
  def ofTimeout(timeout: Duration): Deadline = {
    val now = Time.now
    Deadline(now, now+timeout)
  }

  /**
   * Construct a new deadline, representing the combined deadline
   * `d1` and `d2`. Specifically, the returned deadline has the
   * earliest deadline but the latest timestamp. This represents the
   * strictest deadline and the latest observation.
   */
  def combined(d1: Deadline, d2: Deadline): Deadline =
    Deadline(d1.timestamp max d2.timestamp, d1.deadline min d2.deadline)

  def marshal(deadline: Deadline): Buf = {
    val bytes = new Array[Byte](16)
    ByteArrays.put64be(bytes, 0, deadline.timestamp.inNanoseconds)
    ByteArrays.put64be(bytes, 8, deadline.deadline.inNanoseconds)
    Buf.ByteArray.Owned(bytes)
  }

  def tryUnmarshal(body: Buf): Try[Deadline] = {
    if (body.length != 16) 
      return Throw(new IllegalArgumentException("Invalid body"))
    
    val bytes = Buf.ByteArray.Owned.extract(body)
    val timestamp = ByteArrays.get64be(bytes, 0)
    val deadline = ByteArrays.get64be(bytes, 8)
    
    Return(Deadline(Time.fromNanoseconds(timestamp), Time.fromNanoseconds(deadline)))
  }
}
