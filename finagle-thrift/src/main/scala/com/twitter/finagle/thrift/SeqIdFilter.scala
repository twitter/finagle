package com.twitter.finagle.thrift

import com.twitter.finagle.{Service, SimpleFilter, TransportException}
import com.twitter.util.{Time, Future, Try, Return, Throw}
import scala.util.Random

/**
 * Indicates that a Thrift response did not have the correct sequence
 * ID according to that assigned by [[com.twitter.finagle.thrift.SeqIdFilter]]
 * on the corresponding request.
 */
case class SeqMismatchException(id: Int, expected: Int) extends TransportException {
  override def toString = "SeqMismatchException: got %d, expected %d".format(id, expected)
}

object SeqIdFilter {
  val VersionMask = 0xffff0000
  val Version1 = 0x80010000
}

/**
 * A `Filter` that overrides Thrift request sequence IDs,
 * replacing them with our own randomly-assigned i32s. Upon response receipt,
 * this filter ensures that responses have the correct corresponding sequence ID,
 * failing any requests that do not.
 *
 * @note This only works when using BinaryProtocol.
 */
class SeqIdFilter extends SimpleFilter[ThriftClientRequest, Array[Byte]] {
  import SeqIdFilter._

  // Why random? Since the underlying codec currently does serial
  // dispatching, it doesn't make any difference, but technically we
  // need to ensure that we pick IDs from a free pool.
  private[this] val rng = new Random(Time.now.inMilliseconds)

  private[this] def get32(buf: Array[Byte], off: Int) =
    ((buf(off + 0) & 0xff) << 24) |
      ((buf(off + 1) & 0xff) << 16) |
      ((buf(off + 2) & 0xff) << 8) |
      (buf(off + 3) & 0xff)

  private[this] def put32(buf: Array[Byte], off: Int, x: Int): Unit = {
    buf(off) = (x >> 24 & 0xff).toByte
    buf(off + 1) = (x >> 16 & 0xff).toByte
    buf(off + 2) = (x >> 8 & 0xff).toByte
    buf(off + 3) = (x & 0xff).toByte
  }

  private[this] def badMsg(why: String) = Throw(new IllegalArgumentException(why))

  private[this] def getAndSetId(buf: Array[Byte], newId: Int): Try[Int] = {
    if (buf.length < 4) return badMsg("short header")
    val header = get32(buf, 0)
    val off = if (header < 0) {
      // [4]header
      // [4]n
      // [n]string
      // [4]seqid
      if ((header & VersionMask) != Version1)
        return badMsg("bad version %d".format(header & VersionMask))
      if (buf.length < 8) return badMsg("short name size")
      4 + 4 + get32(buf, 4)
    } else {
      // [4]n
      // [n]name
      // [1]type
      // [4]seqid
      4 + header + 1
    }

    if (buf.length < off + 4) return badMsg("short buffer")

    val currentId = get32(buf, off)
    put32(buf, off, newId)
    Return(currentId)
  }

  def apply(
    req: ThriftClientRequest,
    service: Service[ThriftClientRequest, Array[Byte]]
  ): Future[Array[Byte]] =
    if (req.oneway) service(req)
    else {
      val reqBuf = req.message.clone()
      val id = rng.nextInt()
      val givenId = getAndSetId(reqBuf, id) match {
        case Return(id) => id
        case Throw(exc) => return Future.exception(exc)
      }
      val newReq = new ThriftClientRequest(reqBuf, req.oneway)

      service(newReq) flatMap { resBuf =>
        // We know it's safe to mutate the response buffer since the
        // codec never touches it again.
        getAndSetId(resBuf, givenId) match {
          case Return(`id`) => Future.value(resBuf)
          case Return(badId) => Future.exception(SeqMismatchException(badId, id))
          case Throw(exc) => Future.exception(exc)
        }
      }
    }
}
