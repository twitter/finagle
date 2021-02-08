package com.twitter.finagle.mux

import com.twitter.io.Buf
import com.twitter.finagle.Path

/** A mux request. */
sealed trait Request {

  /** The destination name specified by Tdispatch requests. Otherwise, Path.empty */
  def destination: Path

  /** The payload of the request. */
  def body: Buf

  /** The contexts of the request. */
  def contexts: Seq[(Buf, Buf)]
}

object Request {
  private case class Impl(destination: Path, contexts: Seq[(Buf, Buf)], body: Buf) extends Request {
    override def toString =
      s"mux.Request.Impl(dest=$destination, contexts=[${contexts.mkString(", ")}], body=$body)"
  }

  val empty: Request = Impl(Path.empty, Nil, Buf.Empty)

  /**
   * Variant to create a [[Request]] which only requires a 'payload' (i.e. [[Request]] 'body'). This
   * version should likely only be used when testing.
   */
  def apply(payload: Buf): Request = apply(Path.empty, Nil, payload)

  def apply(dst: Path, payload: Buf): Request = apply(dst, Nil, payload)

  def apply(dst: Path, ctxts: Seq[(Buf, Buf)], payload: Buf): Request = Impl(dst, ctxts, payload)
}

/** For java compatibility */
object Requests {
  val empty: Request = Request.empty

  /**
   * Variant to create a [[Request]] which only requires a 'payload' (i.e. [[Request]] 'body'). This
   * version should likely only be used when testing.
   */
  def make(payload: Buf): Request = make(Path.empty, Nil, payload)

  def make(dst: Path, payload: Buf): Request = make(dst, Nil, payload)

  def make(dst: Path, contexts: Seq[(Buf, Buf)], payload: Buf): Request =
    Request(dst, contexts, payload)
}
