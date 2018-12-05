package com.twitter.finagle.redis.protocol

import com.twitter.io.{Buf, ByteReader}
import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

/**
 * Thread-safe, stateful, asynchronous Redis decoder.
 */
private[redis] final class StageDecoder(init: Stage) {

  private[this] final class Acc(
    var n: Long,
    val replies: ListBuffer[Reply],
    val finish: List[Reply] => Reply)

  import Stage._

  private[this] var remaining = Buf.Empty
  private[this] var stack = List.empty[Acc]
  private[this] var current = init

  /**
   * Returns a [[Reply]] or `null` if it's not enough data in the
   * underlying buffer.
   *
   * @note Passing `Buf.Empty` to this function means "decode from whatever
   *       is in the underlying buffer so far".
   */
  def absorb(buf: Buf): Reply = synchronized {
    // Absorb the new buffer.
    val reader = ByteReader(remaining.concat(buf))
    try {
      // Decode the next reply if possible.
      val result = decodeNext(current, reader)
      // preserve any unconsumed data
      remaining = reader.readAll()
      result
    } finally reader.close()
  }

  // Tries its best to decode the next _full_ reply or returns `null` if
  // there is not enough data in the input buffer.
  @tailrec
  private[this] def decodeNext(stage: Stage, reader: ByteReader): Reply = stage(reader) match {
    case NextStep.Incomplete =>
      // The decoder is starving so we capture the current state
      // and fail-fast with `null`.
      current = stage
      null
    case NextStep.Goto(nextStage) => decodeNext(nextStage, reader)
    case NextStep.Emit(reply) =>
      stack match {
        case Nil =>
          // We finish decoding of a single reply so reset the state.
          current = init
          reply
        case acc :: rest if acc.n == 1 =>
          stack = rest
          acc.replies += reply
          decodeNext(Stage.const(NextStep.Emit(acc.finish(acc.replies.toList))), reader)
        case acc :: _ =>
          acc.n -= 1
          acc.replies += reply
          decodeNext(init, reader)
      }
    case NextStep.Accumulate(n, finish) =>
      stack = new Acc(n, ListBuffer.empty[Reply], finish) :: stack
      decodeNext(init, reader)
  }
}
