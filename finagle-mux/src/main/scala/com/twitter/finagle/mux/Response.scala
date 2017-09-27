package com.twitter.finagle.mux

import com.twitter.io.Buf

/** A mux response. */
sealed trait Response {

  /** The payload of the response. */
  def body: Buf

  /** The contexts of the response. */
  def contexts: Seq[(Buf, Buf)]
}

object Response {
  private case class Impl(contexts: Seq[(Buf, Buf)], body: Buf) extends Response {
    override def toString = s"mux.Response.Impl(contexts=[${contexts.mkString(", ")}], body=$body)"
  }

  val empty: Response = Impl(Nil, Buf.Empty)

  def apply(buf: Buf): Response = apply(Nil, buf)

  def apply(ctxts: Seq[(Buf, Buf)], buf: Buf): Response = Impl(ctxts, buf)
}

/** For java compatibility */
object Responses {
  val empty: Response = Response.empty

  def make(payload: Buf): Response = make(Nil, payload)

  def make(contexts: Seq[(Buf, Buf)], payload: Buf): Response = Response(contexts, payload)
}
