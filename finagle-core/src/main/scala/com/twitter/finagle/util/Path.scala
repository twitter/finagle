package com.twitter.finagle.util

/**
 * @note this is inefficient.
 */
object Path {
  def split(path: String): Seq[String] =
    path.split('/').filter(_.nonEmpty)

  def mk(cs: Seq[String]): String =
    "/"+cs.mkString("/")

  def clean(path: String): String =
    mk(split(path))

  def join(a: String, b: String): String =
    clean(a+"/"+b)
}
