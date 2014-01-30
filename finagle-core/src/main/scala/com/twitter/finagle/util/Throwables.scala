package com.twitter.finagle.util

import scala.annotation.tailrec

object Throwables {
  /**
   * Traverse a nested `Throwable`, flattening all causes into a Seq of
   * classname strings.
   */
  def mkString(ex: Throwable): Seq[String] = {
    @tailrec def rec(ex: Throwable, result: Seq[String]): Seq[String] = {
      if (ex eq null)
        result
      else
        rec(ex.getCause, result :+ ex.getClass.getName)
    }

    rec(ex, Seq.empty)
  }
}
