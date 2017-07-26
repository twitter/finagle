package com.twitter.finagle.context

import com.twitter.finagle.util.ByteArrays
import com.twitter.io.Buf
import com.twitter.util.{Duration, Return, Throw, Time, Try}

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
  def remaining: Duration = deadline - Time.now
}

/**
 * A broadcast context for deadlines.
 */
object Deadline extends Contexts.broadcast.Key[Deadline]("com.twitter.finagle.Deadline") {

  /**
   * Returns the current request's deadline, if set.
   */
  def current: Option[Deadline] =
    Contexts.broadcast.get(Deadline)

  /**
   * Construct a deadline from a timeout.
   */
  def ofTimeout(timeout: Duration): Deadline = {
    val now = Time.now
    Deadline(now, now + timeout)
  }

  /**
   * Construct a new deadline, representing the combined deadline
   * `d1` and `d2`. Specifically, the returned deadline has the
   * earliest deadline but the latest timestamp. This represents the
   * strictest deadline and the latest observation.
   */
  def combined(d1: Deadline, d2: Deadline): Deadline =
    Deadline(d1.timestamp max d2.timestamp, d1.deadline min d2.deadline)

  /**
   * Marshal deadline to byte buffer, deadline.timestamp and deadline.deadline
   * must not be Time.Top, Time.Bottom or Time.Undefined
   */
  def marshal(deadline: Deadline): Buf = {
    val bytes = new Array[Byte](16)
    ByteArrays.put64be(bytes, 0, deadline.timestamp.inNanoseconds)
    ByteArrays.put64be(bytes, 8, deadline.deadline.inNanoseconds)
    Buf.ByteArray.Owned(bytes)
  }

  private[this] def readBigEndianLong(b: Buf, offset: Int): Long = {
    ((b.get(offset) & 0xff).toLong << 56) |
      ((b.get(offset + 1) & 0xff).toLong << 48) |
      ((b.get(offset + 2) & 0xff).toLong << 40) |
      ((b.get(offset + 3) & 0xff).toLong << 32) |
      ((b.get(offset + 4) & 0xff).toLong << 24) |
      ((b.get(offset + 5) & 0xff).toLong << 16) |
      ((b.get(offset + 6) & 0xff).toLong << 8) |
      (b.get(offset + 7) & 0xff).toLong
  }

  def tryUnmarshal(body: Buf): Try[Deadline] = {
    if (body.length != 16)
      return Throw(
        new IllegalArgumentException(s"Invalid body. Length ${body.length} but required 16")
      )

    val timestamp = readBigEndianLong(body, 0)
    val deadline = readBigEndianLong(body, 8)

    Return(Deadline(Time.fromNanoseconds(timestamp), Time.fromNanoseconds(deadline)))
  }
}
