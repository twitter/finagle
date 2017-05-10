package com.twitter.finagle.context

import com.twitter.io.{Buf, ByteReader, ByteWriter}
import com.twitter.util.{Return, Throw, Try}

/**
 * Retries contains the number of times a request has been retried.
 *
 * @param retries the number of retries
 */
private[finagle] case class Retries(val retries: Int)

private[finagle] object Retries
  extends Contexts.broadcast.Key[Retries]("com.twitter.finagle.Retries")
{

  def current: Option[Retries] =
    Contexts.broadcast.get(Retries)

  override def marshal(retries: Retries): Buf = {
    val bw: ByteWriter = ByteWriter.fixed(4)
    bw.writeIntBE(retries.retries)
    bw.owned()
  }

  override def tryUnmarshal(buf: Buf): Try[Retries] = {
    if (buf.length != 4) {
      Throw(new IllegalArgumentException(
        s"Could not extract Retries from Buf. Length ${buf.length} but required 4"))
    } else {
      val retries: Int = ByteReader(buf).readIntBE()
      Return(Retries(retries))
    }
  }
}
