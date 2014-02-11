package com.twitter.finagle.util

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

object Throwables {
  /**
   * Traverse a nested `Throwable`, flattening all causes into a Seq of
   * classname strings.
   */
  def mkString(ex: Throwable): Seq[String] = {
    @tailrec def rec(ex: Throwable, buf: ArrayBuffer[String]): Seq[String] = {
      if (ex eq null)
        buf.result
      else
        rec(ex.getCause, buf += ex.getClass.getName)
    }

    rec(ex, ArrayBuffer.empty)
  }
}
