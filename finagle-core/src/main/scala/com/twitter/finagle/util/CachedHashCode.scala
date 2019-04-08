package com.twitter.finagle.util

import scala.util.hashing.MurmurHash3

/**
 * A mix-in that lazily computes, then caches, the result of
 * the object's `hashCode`. The computation of the hash code is
 * delegated to `computeHashCode`.
 *
 * This trait should only be added to (effectively) immutable objects
 * as subsequent calls to `hashCode` will not reflect changes
 * to the underlying values used in the calculation.
 *
 * @see [[CachedHashCode.ForCaseClass]] when mixing into a case class (or any
 *     `scala.Product`).
 * @see [[CachedHashCode.ForClass]] for other classes.
 *
 * @note this implementation assumes that multiple calls to `computeHashCode`
 *       are copacetic and as such does not protect against that.
 *
 * @note a sentinel value is used to indicate that the hash code has not yet
 *       been calculated. if `computeHashCode` happens to return that sentinel
 *       a different sentinel is used in its stead.
 */
private[finagle] sealed trait CachedHashCode {
  import CachedHashCode._

  private[this] var cachedHashCode = NotComputed

  /**
   * How `hashCode` is computed.
   */
  protected def computeHashCode: Int

  final override def hashCode: Int = {
    if (cachedHashCode == NotComputed) {
      val computed = computeHashCode
      cachedHashCode = if (computed == NotComputed) ComputedCollision else computed
    }
    cachedHashCode
  }
}

/**
 */
private[finagle] object CachedHashCode {

  /**
   * An "uninteresting" number used to signify that the hashCode
   * has not yet been computed.
   *
   * If the `computeHashCode` returns this value, `ComputedCollision`
   * which is different "uninteresting" number is used instead.
   *
   * Exposed for testing purposes.
   */
  private[util] val NotComputed: Int = -777666777

  /** Exposed for testing purposes. */
  private[util] val ComputedCollision: Int = -88899988

  /**
   * Defaults to using `super.hashCode` for `computeHashCode`.
   */
  trait ForClass extends CachedHashCode {
    protected def computeHashCode: Int = super.hashCode
  }

  /**
   * Defaults to using `MurmurHash3.productHash` for `computeHashCode`.
   * This is the same as the generated `hashCode` for case classes.
   */
  trait ForCaseClass extends CachedHashCode { _: Product =>
    final protected def computeHashCode: Int = MurmurHash3.productHash(this)
  }

}
