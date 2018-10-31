package com.twitter.finagle.context

import com.twitter.io.{Buf, BufByteWriter, ByteReader}
import com.twitter.util.{Return, Throw, Try}

/**
 * Retries contains the number of times a request has been retried.
 *
 * @param attempt which retry attempt this is. Will be 0
 *                if the request is not a retry.
 */
case class Retries(attempt: Int)

object Retries extends Contexts.broadcast.Key[Retries]("com.twitter.finagle.Retries") {

  def current: Option[Retries] =
    Contexts.broadcast.get(Retries)

  override def marshal(retries: Retries): Buf = {
    val bw = BufByteWriter.fixed(4)
    bw.writeIntBE(retries.attempt)
    bw.owned()
  }

  override def tryUnmarshal(buf: Buf): Try[Retries] = {
    if (buf.length != 4) {
      Throw(
        new IllegalArgumentException(
          s"Could not extract Retries from Buf. Length ${buf.length} but required 4"
        )
      )
    } else {
      val attempt: Int = ByteReader(buf).readIntBE()
      Return(Retries(attempt))
    }
  }
}
