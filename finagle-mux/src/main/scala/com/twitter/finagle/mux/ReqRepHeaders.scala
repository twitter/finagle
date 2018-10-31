package com.twitter.finagle.mux

import com.twitter.finagle.context.Contexts
import com.twitter.finagle.context.Contexts.broadcast
import com.twitter.finagle.mux.transport.Message.RdispatchOk
import com.twitter.io.{Buf, BufByteWriter, ByteReader}
import com.twitter.logging.Logger
import com.twitter.util.{Return, Throw, Try}
import scala.util.control.NonFatal

private[mux] object ReqRepHeaders {

  private val logger = Logger.get()

  /**
   * Maximum length of encoded application headers
   *
   * We use a single mux `Tdispatch` header field to encapsulate all the application
   * headers which provides a way to isolate them from the traditionally defined
   * broadcast context entries. However, because header keys and values are length
   * delimited with a signed-short (i16), we are limited to that length for the
   * encoded headers.
   */
  val MaxEncodedLength = Short.MaxValue

  // We use a broadcast key instead of a `Buf` like we do for response headers
  // because it lets us use the collection manipulation tools of the broadcast
  // context instead of having to do it manually.
  private val RequestBroadcastKey: broadcast.Key[Seq[(Buf, Buf)]] =
    new broadcast.Key[Seq[(Buf, Buf)]]("com.twitter.finagle.thrift.Request.headers") {
      def marshal(values: Seq[(Buf, Buf)]): Buf = encode(values)
      def tryUnmarshal(buf: Buf): Try[Seq[(Buf, Buf)]] =
        try Return(decode(buf))
        catch { case NonFatal(t) => Throw(t) }
    }

  private val ResponseBroadcastKey: Buf = Buf.Utf8("com.twitter.finagle.thrift.Response.headers")

  private val appHeadersPredicate: ((Buf, Buf)) => Boolean = {
    case (k, _) =>
      k == ReqRepHeaders.ResponseBroadcastKey
  }

  def responseHeaders(rep: RdispatchOk): Seq[(Buf, Buf)] = {
    rep.contexts.find(appHeadersPredicate) match {
      case Some((_, v)) => ReqRepHeaders.decode(v)
      case None => Nil
    }
  }

  /** Encode the context entries used by a `Tdispatch` request
   *
   * The resulting collection of context entries will include all the broadcast contexts
   * in scope __and__ the request headers encapsulated as a new entry, if they exist.
   */
  def toDispatchContexts(req: Request): Iterable[(Buf, Buf)] = {
    if (req.contexts.nonEmpty) {
      Contexts.broadcast.let(ReqRepHeaders.RequestBroadcastKey, req.contexts) {
        Contexts.broadcast.marshal()
      }
    } else
      Contexts.broadcast.letClear(ReqRepHeaders.RequestBroadcastKey) {
        Contexts.broadcast.marshal()
      }
  }

  /**
   * Encode the response into the `Rdispatch` context entries
   *
   * The resulting collection of context entries only includes the response headers
   * encapsulated in one entry to add symmetry with the request context encoding.
   */
  def toDispatchContexts(rep: Response): Iterable[(Buf, Buf)] = {
    if (rep.contexts.isEmpty) Nil
    else {
      (ReqRepHeaders.ResponseBroadcastKey -> ReqRepHeaders.encode(rep.contexts)) :: Nil
    }
  }

  /**
   * Execute the provided block with the request application headers
   *
   * Request application headers are extracted and removed from the broadcast
   * context and passed to the provided lambda.
   */
  def withApplicationHeaders[T](f: Seq[(Buf, Buf)] => T): T = {
    Contexts.broadcast.get(ReqRepHeaders.RequestBroadcastKey) match {
      case None => f(Nil)
      case Some(hdrs) =>
        Contexts.broadcast.letClear(ReqRepHeaders.RequestBroadcastKey) {
          f(hdrs)
        }
    }
  }

  /**
   * Encode headers to a `Buf` representation
   *
   * @note headers that overflow the maximum representation are dropped.
   */
  def encode(headers: Seq[(Buf, Buf)]): Buf = {
    val allHeaderSize = ContextCodec.encodedLength(headers.iterator)
    if (allHeaderSize <= MaxEncodedLength) {
      val bw = BufByteWriter.fixed(allHeaderSize)
      ContextCodec.encode(bw, headers.iterator)
      bw.owned()
    } else {
      // we need to cull excess headers. We drop from the tail, arbitrarily.
      logger.warning(
        s"Application headers exceeded maximum length. Maximum: $MaxEncodedLength. " +
          s"Observed: $allHeaderSize. Excess headers will be dropped."
      )
      var size = 0
      val iter = headers.iterator.takeWhile {
        case (k, v) =>
          size += 4 + k.length + v.length // 4, 2 each for each of the length fields
          size <= MaxEncodedLength
      }

      val bw = BufByteWriter.dynamic()
      ContextCodec.encode(bw, iter)
      bw.owned()
    }
  }

  /** Decode the application headers from the `Buf` representation */
  def decode(buf: Buf): Seq[(Buf, Buf)] = ContextCodec.decodeAll(ByteReader(buf))
}
