package com.twitter.finagle.thrift

import com.twitter.finagle.{Service, SimpleFilter, TransportException}
import com.twitter.util.{Time, Future}
import scala.util.Random

case class SeqMismatchException(id: Int, expected: Int) extends TransportException {
  override def toString = "SeqMismatchException: got %d, expected %d".format(id, expected)
}

/**
 * A filter to override the input sequence ids, replacing them with
 * ones of our own provenance. We perform checking on these and fail
 * accordingly.
 *
 * Note: This only works when using BinaryProtocol, but will become
 * generic with mux support.
 */
class SeqIdFilter extends SimpleFilter[ThriftClientRequest, Array[Byte]] {
  // Why random? Since the underlying codec currently does serial
  // dispatching, it doesn't make any difference, but technically we
  // need to ensure that we pick IDs from a free pool.
  private[this] val rng = new Random(Time.now.inMilliseconds)

  private[this] def get32(buf: Array[Byte], off: Int) =
    ((buf(off+0) & 0xff) << 24) |
    ((buf(off+1) & 0xff) << 16) |
    ((buf(off+2) & 0xff) << 8) |
    (buf(off+3) & 0xff)

  private[this] def put32(buf: Array[Byte], off: Int, x: Int) {
    buf(off) = (x>>24 & 0xff).toByte
    buf(off+1) = (x>>16 & 0xff).toByte
    buf(off+2) = (x>>8 & 0xff).toByte
    buf(off+3) = (x & 0xff).toByte
  }

  private[this] def getAndSetId(buf: Array[Byte], newId: Int): Option[Int] = {
    if (buf.size < 4) return None
    val size = get32(buf, 0)
    val off = if (size < 0) {
      if (buf.size < 8) return None
      8+get32(buf, 4)
    } else
      4+size+1

    if (buf.size<off+4) return None

    val currentId = get32(buf, off)
    put32(buf, off, newId)
    Some(currentId)
  }

  def apply(req: ThriftClientRequest, service: Service[ThriftClientRequest, Array[Byte]]): Future[Array[Byte]] =
    if (req.oneway) service(req) else {
      val buf = req.message
      val id = rng.nextInt()
      val givenId = getAndSetId(buf, id) getOrElse {
        return Future.exception(new IllegalArgumentException("bad TMessage"))
      }

      service(req) flatMap { buf =>
        getAndSetId(buf, givenId) match {
          case Some(`id`) => Future.value(buf)
          case Some(badId) => Future.exception(SeqMismatchException(badId, id))
          case None => Future.exception(new IllegalArgumentException("bad TMessage"))
        }
      }
    }
}
