package com.twitter.finagle.toggle

import com.twitter.finagle.toggle.Toggle.Metadata
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.varargs
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * A collection of Int-typed [[Toggle toggles]] which can be
 * used to build a form of feature toggles which allow for modifying
 * behavior without changing code.
 *
 * Expected usage is for code to have [[Toggle toggles]] passed into
 * their constructors instead of dynamically creating new [[Toggle toggles]]
 * on every call.
 *
 * @see [[Toggle]]
 * @see `LoadedToggleMap` and `FinagleToggleMap` in `finagle-core`
 *     for typical usage entry points.
 * @see [[http://martinfowler.com/articles/feature-toggles.html Feature Toggles]]
 *      for detailed discussion on the topic.
 */
abstract class ToggleMap { self =>

  /**
   * Get a [[Toggle]] for this `id`.
   *
   * The `Toggle.isDefined` method should return `false` if the
   * [[ToggleMap]] does not know about that [[Toggle]]
   * or it is currently not "operating" on that `id`.
   *
   * @param id the identifying name of the `Toggle`.
   *           These should generally be fully qualified names to avoid conflicts
   *           between libraries. For example, "com.twitter.finagle.CoolThing".
   */
  def apply(id: String): Toggle[Int]

  def iterator: Iterator[Toggle.Metadata]

  /**
   * Creates a [[ToggleMap]] which uses `this` before `that`.
   *
   * [[apply]] returns a [[Toggle]] that uses the [[Toggle]] from `this`
   * if it `isDefinedAt` for the input, before trying `that`.
   *
   * [[iterator]] includes metadata from both `self` and `that`,
   * with `self`'s metadata taking precedence on conflicting ids.
   */
  def orElse(that: ToggleMap): ToggleMap = {
    new ToggleMap {
      override def toString: String =
        s"${self.toString}.orElse(${that.toString})"

      def apply(id: String): Toggle[Int] = {
        self(id).orElse(that(id))
      }

      def iterator: Iterator[Metadata] = {
        val byName = mutable.Map.empty[String, Toggle.Metadata]
        that.iterator.foreach { md =>
          byName.put(md.id, md)
        }
        self.iterator.foreach { md =>
          byName.put(md.id, md)
        }
        byName.valuesIterator
      }
    }
  }
}

object ToggleMap {

  /**
   * The [[ToggleMap]] interface is read only and this
   * is the mutable side of it.
   */
  trait MutableToggleMap extends ToggleMap {

    /**
     * Add or replace the [[Toggle]] for this `id` with a
     * [[Toggle]] that returns `true` for a `fraction` of the inputs.
     *
     * @param id the identifying name of the `Toggle`.
     *             These should generally be fully qualified names to avoid conflicts
     *             between libraries. For example, "com.twitter.finagle.CoolThing".
     * @param fraction must be within `0.0–1.0`, inclusive. If not, the operation
     *                 is ignored.
     */
    def put(id: String, fraction: Double): Unit

    /**
     * Remove the [[Toggle]] for this `id`.
     *
     * This is a no-op for missing values.
     *
     * @param id the identifying name of the `Toggle`.
     *           These should generally be fully qualified names to avoid conflicts
     *           between libraries. For example, "com.twitter.finagle.CoolThing".
     */
    def remove(id: String): Unit
  }

  /**
   * Create a [[Toggle]] where `fraction` of the inputs will return `true.`
   *
   * @param id the name of the Toggle which is used to mix
   *           where along the universe of Ints does the range fall.
   * @param fraction the fraction, from 0.0 - 1.0 (inclusive), of Ints
   *          to return `true`. If outside of that range, a
   *          `java.lang.IllegalArgumentException` will be thrown.
   */
  private[toggle] def fractional(id: String, fraction: Double): Toggle[Int] = {
    Toggle.validateId(id)
    Toggle.validateFraction(fraction)

    // we want a continuous range within the space of Int.MinValue
    // to Int.MaxValue, including overflowing Max.
    // By mapping the range to a Long and then mapping this into the
    // space of Ints we create a Toggle that is both space efficient
    // as well as quick to respond to `apply`.

    // within a range of [0, Int.MaxValue*2]
    val range: Long = ((1L << 32) * fraction).toLong

    // We want to use `id` as an input into the function so
    // that ints have different likelihoods depending on the toggle's id.
    // Without this, every Toggle's range would start at 0.
    // The input to many toggle's may be something consistent per node,
    // say a machine name. So without the offset, nodes that hash to
    // close to 0 will be much more likely to have most or all toggles
    // turned on. by using the id as an offset, we can shift this and
    // make them be more evenly distributed.
    val start = id.hashCode
    val end: Int = (start + range).toInt

    if (range == 0) {
      Toggle.False // 0%
    } else if (start == end) {
      Toggle.True // 100%
    } else if (start <= end) {
      // the range is contiguous without overflows.
      Toggle(id, { case i => i >= start && i <= end })
    } else {
      // the range overflows around Int.MaxValue
      Toggle(id, { case i  => i >= start || i <= end })
    }
  }

  /**
   * Create a [[ToggleMap]] out of the given [[ToggleMap ToggleMaps]].
   *
   * If `toggleMaps` is empty, [[NullToggleMap]] will be returned.
   */
  @varargs
  def of(toggleMaps: ToggleMap*): ToggleMap = {
    if (toggleMaps.isEmpty) {
      NullToggleMap
    } else {
      toggleMaps.foldLeft(toggleMaps.head) { case (acc, tm) =>
        acc.orElse(tm)
      }
    }
  }

  private[this] val NoFractionAndToggle = (Double.NaN, Toggle.Undefined)

  private class MutableToggle(id: String) extends Toggle[Int] {
    Toggle.validateId(id)

    private[this] val fractionAndToggle =
      new AtomicReference[(Double, Toggle[Int])](NoFractionAndToggle)

    override def toString: String = s"MutableToggle($id)"

    private[ToggleMap] def currentFraction: Double =
      fractionAndToggle.get()._1

    private[ToggleMap] def setFraction(fraction: Double): Unit = {
      val fAndT: (Double, Toggle[Int]) = if (Toggle.isValidFraction(fraction)) {
        (fraction, fractional(id, fraction))
      } else {
        NoFractionAndToggle
      }
      fractionAndToggle.set(fAndT)
    }

    def isDefinedAt(t: Int): Boolean =
      fractionAndToggle.get()._2.isDefinedAt(t)

    def apply(t: Int): Boolean =
      fractionAndToggle.get()._2(t)
  }

  private[toggle] def newMutable(): MutableToggleMap = new MutableToggleMap {

    override def toString: String =
      s"ToggleMap.Mutable(${System.identityHashCode(this)})"

    // There will be minimal updates, so we can use a low concurrency level,
    // which makes the footprint smaller.
    private[this] val toggles =
      new ConcurrentHashMap[String, MutableToggle](32, 0.75f, 1)

    private[this] def toggleFor(id: String): MutableToggle = {
      val curr = toggles.get(id)
      if (curr != null) {
        curr
      } else {
        val newToggle = new MutableToggle(id)
        val prev = toggles.putIfAbsent(id, newToggle)
        if (prev == null)
          newToggle
        else
          prev
      }
    }

    def apply(id: String): Toggle[Int] =
      toggleFor(id)

    def iterator: Iterator[Toggle.Metadata] =
      toggles.asScala.collect {
        case (id, toggle) if Toggle.isValidFraction(toggle.currentFraction) =>
          Toggle.Metadata(id, toggle.currentFraction, None)
      }.toIterator

    def put(id: String, fraction: Double): Unit = {
      if (Toggle.isValidFraction(fraction)) {
        toggleFor(id).setFraction(fraction)
      }
    }

    def remove(id: String): Unit =
      toggleFor(id).setFraction(Double.NaN)
  }

  /**
   * The shared, mutable, [[ToggleMap]] that can be manipulated
   * by service owners.
   */
  val mutable: MutableToggleMap = newMutable()

  /**
   * A [[ToggleMap]] that is backed by a [[com.twitter.app.GlobalFlag GlobalFlag]],
   * [[flag.overrides]].
   *
   * Its [[Toggle Toggles]] will reflect changes to the underlying `Flag` which
   * enables usage in tests.
   *
   * Fractions that are out of range (outside of `[0.0-1.0]`) will be
   * ignored.
   */
  val flags: ToggleMap = new ToggleMap {

    override def toString: String = "ToggleMap.Flags"

    private[this] def fractions: Map[String, Double] =
      flag.overrides()

    private[this] class FlagToggle(id: String) extends Toggle[Int] {
      Toggle.validateId(id)

      private[this] val fractionAndToggle =
        new AtomicReference[(Double, Toggle[Int])](NoFractionAndToggle)

      override def toString: String = s"FlagToggle($id)"

      def isDefinedAt(t: Int): Boolean =
        fractions.get(id) match {
          case Some(f) if Toggle.isValidFraction(f) => true
          case _ => false
        }

      def apply(t: Int): Boolean = {
        fractions.get(id) match {
          case Some(f) if Toggle.isValidFraction(f) =>
            val prev = fractionAndToggle.get()
            val toggle =
              if (f == prev._1) {
                // we can use the cached toggle since the fraction matches
                prev._2
              } else {
                val newToggle = fractional(id, f)
                fractionAndToggle.compareAndSet(prev, (f, newToggle))
                newToggle
              }
            toggle(t)
          case _ =>
            throw new IllegalStateException(s"$this not defined for input: $t")

        }
      }
    }

    def apply(id: String): Toggle[Int] =
      new FlagToggle(id)

    def iterator: Iterator[Toggle.Metadata] =
      fractions.iterator.collect { case (id, f) if Toggle.isValidFraction(f) =>
        Toggle.Metadata(id, f, None)
      }

  }

  private[toggle] trait Proxy extends ToggleMap {
    protected def underlying: ToggleMap

    override def toString: String = underlying.toString
    def apply(id: String): Toggle[Int] = underlying(id)
    def iterator: Iterator[Metadata] = underlying.iterator
  }

}
