package com.twitter.finagle.context

import com.twitter.io.Buf
import com.twitter.io.BufByteWriter
import com.twitter.io.ByteReader
import com.twitter.util.Return
import com.twitter.util.Throw
import com.twitter.util.Try

/**
 * Requeues contains the number of times a request has been requeued.
 *
 * @param attempt which retry attempt this is. Will be 0
 *                if the request is not a requeue.
 */
case class Requeues(attempt: Int)

/**
 * Note: The context id is "c.t.f.Retries" and not "c.t.f.Requeues" because we have renamed the
 * Retries context to Requeues (to reflect what it actually contains, which is the requeues), but we
 * don't want to break existing users/deployments using this context by changing the key.
 */
object Requeues extends Contexts.broadcast.Key[Requeues]("com.twitter.finagle.Retries") {

  def current: Option[Requeues] =
    Contexts.broadcast.get(Requeues)

  override def marshal(requeues: Requeues): Buf = {
    val bw = BufByteWriter.fixed(4)
    bw.writeIntBE(requeues.attempt)
    bw.owned()
  }

  override def tryUnmarshal(buf: Buf): Try[Requeues] = {
    if (buf.length != 4) {
      Throw(
        new IllegalArgumentException(
          s"Could not extract Requeues from Buf. Length ${buf.length} but required 4"
        )
      )
    } else {
      val attempt: Int = ByteReader(buf).readIntBE()
      Return(Requeues(attempt))
    }
  }
}
