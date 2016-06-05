package com.twitter.finagle.mux

import com.twitter.io.Buf

/** A mux response. */
sealed trait Response {
  /** The payload of the response. */
  def body: Buf
}

object Response {
  private case class Impl(body: Buf) extends Response {
    override def toString = s"mux.Response.Impl($body)"
  }

  val empty: Response = Impl(Buf.Empty)

  def apply(buf: Buf): Response = Impl(buf)
}

/** For java compatibility */
object Responses {
  val empty: Response = Response.empty

  def make(payload: Buf): Response = Response(payload)
}