package com.twitter.finagle.mux

import com.twitter.io.Buf
import com.twitter.finagle.Path

/** A mux request. */
sealed trait Request {
  /** The destination name specified by Tdispatch requests. Otherwise, Path.empty */
  def destination: Path

  /** The payload of the request. */
  def body: Buf
}

object Request {
  private case class Impl(destination: Path, body: Buf) extends Request {
    override def toString = s"mux.Request.Impl($destination, $body)"
  }

  val empty: Request = Impl(Path.empty, Buf.Empty)

  def apply(dst: Path, payload: Buf): Request = Impl(dst, payload)
}

/** For java compatibility */
object Requests {
  val empty: Request = Request.empty

  def make(dst: Path, payload: Buf): Request = Request(dst, payload)
}