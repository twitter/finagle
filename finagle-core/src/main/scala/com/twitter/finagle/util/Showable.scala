package com.twitter.finagle.util

/**
 * Trait showable is a type-class for showing parsable
 * representations of objects.
 */
trait Showable[-T] {
  def show(t: T): String
}

object Showable {
  def show[T](t: T)(implicit showable: Showable[T]): String =
    showable.show(t)
}
