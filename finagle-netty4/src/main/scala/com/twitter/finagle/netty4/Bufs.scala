package com.twitter.finagle.netty4

import com.twitter.io.Buf

private[finagle] object Bufs {

  private[this] val rFn: Buf => Unit = releaseDirect(_)

  /**
   * Decrement the ref-count of [[Buf]] instances which are
   * backed by direct netty byte buffers.
   */
  def releaseDirect(msg: Buf): Unit = msg match {
    case bbb: ByteBufAsBuf if bbb.underlying.isDirect =>
      bbb.underlying.release()

    case comp: Buf.Composite =>
      comp.bufs.foreach(rFn)

    case _ =>
      ()
  }

  private[this] val copyFn: Buf => Buf = copyAndReleaseDirect(_)
  /**
   * Releases direct byte buffers and returns an on-heap copy. Other byte buffers
   * arguments are returned without change.
   */
  def copyAndReleaseDirect(msg: Buf): Buf = msg match {
    case bbb: ByteBufAsBuf if bbb.underlying.isDirect =>
      val bytes = new Array[Byte](bbb.underlying.readableBytes)
      bbb.underlying.readBytes(bytes)
      bbb.underlying.release()
      Buf.ByteArray.Owned(bytes)

    case comp: Buf.Composite =>
      Buf.apply(comp.bufs.map(copyFn))

    case _ =>
      msg
  }

}