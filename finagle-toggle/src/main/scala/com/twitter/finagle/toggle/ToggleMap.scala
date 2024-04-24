package com.twitter.finagle.toggle

import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.toggle.Toggle.Metadata
import com.twitter.logging.Logger
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import java.util.zip.CRC32
import java.util.{function => juf}
import java.{lang => jl}
import scala.annotation.varargs
import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.collection.mutable
import scala.util.hashing.MurmurHash3

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
 * @see [[ServiceLoadedToggleMap]] and [[StandardToggleMap]] for typical usage
 *      entry points.
 * @see [[https://martinfowler.com/articles/feature-toggles.html Feature Toggles]]
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
   *
   * @see [[get]] for a Java friendly version
   */
  def apply(id: String): Toggle

  /**
   * Get a [[Toggle]] for this `id`.  Java-friendly synonym for `apply`.
   *
   * The `Toggle.isDefined` method should return `false` if the
   * [[ToggleMap]] does not know about that [[Toggle]]
   * or it is currently not "operating" on that `id`.
   *
   * @param id the identifying name of the `Toggle`.
   *           These should generally be fully qualified names to avoid conflicts
   *           between libraries. For example, "com.twitter.finagle.CoolThing".
   *
   * @note this returns a `java.lang.Integer` for Java compatibility.
   */
  final def get(id: String): Toggle = apply(id)

  def iterator: Iterator[Toggle.Metadata]

  /**
   * Creates a [[ToggleMap]] which uses `this` before `that`.
   *
   * [[apply]] returns a [[Toggle]] that uses the [[Toggle]] from `this`
   * if it `isDefined` for the input, before trying `that`.
   *
   * [[iterator]] includes metadata from both `self` and `that`,
   * with `self`'s metadata taking precedence on conflicting ids.
   * Note however that if a `ToggleMetadata.description` is not defined on `self`,
   * the description from `that` will be preferred. This is done because many
   * sources of `ToggleMaps` do not have a description defined and we want to
   * surface that information.
   */
  def orElse(that: ToggleMap): ToggleMap = {
    new ToggleMap with ToggleMap.Composite {
      override def toString: String =
        s"${self.toString}.orElse(${that.toString})"

      def apply(id: String): Toggle = {
        self(id).orElse(that(id))
      }

      def iterator: Iterator[Metadata] = {
        val byName = mutable.Map.empty[String, Toggle.Metadata]
        that.iterator.foreach { md => byName.put(md.id, md) }
        self.iterator.foreach { md =>
          val mdWithDesc = md.description match {
            case Some(_) => md
            case None => md.copy(description = byName.get(md.id).flatMap(ToggleMap.MdDescFn))
          }
          byName.put(md.id, mdWithDesc)
        }
        byName.valuesIterator
      }

      def components: Seq[ToggleMap] = {
        Seq(self, that)
      }
    }
  }
}

object ToggleMap {

  /**
   * Used to create a `Toggle` that hashes its inputs to
   * `apply` and `isDefined` in order to promote a relatively even
   * distribution even when the inputs do not have a good distribution.
   *
   * This allows users to get away with using a poor hashing function,
   * such as `String.hashCode`.
   */
  private def hashedToggle(id: String, fn: Int => Boolean, fraction: Double): Toggle.Fractional =
    new Toggle.Fractional(id) {
      override def toString: String = s"Toggle($id)"
      // Each Toggle has a different hash seed so that Toggles are independent
      private[this] val hashSeed = MurmurHash3.stringHash(id)
      private[this] def hash(i: Int): Int = {
        val h = MurmurHash3.mix(hashSeed, i)
        MurmurHash3.finalizeHash(h, 1)
      }
      def isDefined: Boolean = true
      def apply(x: Int): Boolean = fn(hash(x))

      def currentFraction: Double = fraction
    }

  private[this] val MetadataOrdering: Ordering[Toggle.Metadata] =
    Ordering.by((md: Toggle.Metadata) => (md.id, md.fraction))

  /**
   * Creates a [[ToggleMap]] with a `Gauge`, "checksum", which summarizes the
   * current state of the `Toggles` which may be useful for comparing state
   * across a cluster or over time.
   *
   * @param statsReceiver in typical usage by [[StandardToggleMap]], will be
   *                      scoped to "toggles/\$libraryName".
   */
  def observed(toggleMap: ToggleMap, statsReceiver: StatsReceiver): ToggleMap = {
    new ToggleMap with Proxy with Composite {
      private[this] val lastApplied =
        new ConcurrentHashMap[String, AtomicReference[jl.Boolean]]()

      private[this] val checksum = statsReceiver.addGauge("checksum") {
        // crc32 is not a cryptographic hash, but good enough for our purposes
        // of summarizing the current state of the ToggleMap. we only need it
        // to be efficient to compute and have small changes to the input affect
        // the output.
        val crc32 = new CRC32()

        // need a consistent ordering, forcing the sort before computation
        iterator.toIndexedSeq.sorted(MetadataOrdering).foreach { md =>
          crc32.update(md.id.getBytes(UTF_8))
          // convert the md's fraction to a Long and then feed each
          // byte into the crc
          val f = java.lang.Double.doubleToLongBits(md.fraction)
          crc32.update((0xff & f).toInt)
          crc32.update((0xff & (f >> 8)).toInt)
          crc32.update((0xff & (f >> 16)).toInt)
          crc32.update((0xff & (f >> 24)).toInt)
          crc32.update((0xff & (f >> 32)).toInt)
          crc32.update((0xff & (f >> 40)).toInt)
          crc32.update((0xff & (f >> 48)).toInt)
          crc32.update((0xff & (f >> 56)).toInt)
        }
        crc32.getValue.toFloat
      }
      private[this] val toggleValue = statsReceiver.addGauge("fraction") {
        iterator.toIndexedSeq.headOption.map(_.fraction.toFloat).getOrElse(0)
      }

      def underlying: ToggleMap = toggleMap

      override def toString: String =
        s"observed($toggleMap, $statsReceiver)"

      def components: Seq[ToggleMap] =
        Seq(underlying)

      // mixes in `Toggle.Captured` to provide visibility into how
      // toggles are in use at runtime.
      override def apply(id: String): Toggle = {
        val delegate = super.apply(id)
        new Toggle(delegate.id) with Toggle.Captured {
          private[this] val last =
            lastApplied.computeIfAbsent(
              id,
              new juf.Function[String, AtomicReference[jl.Boolean]] {
                def apply(t: String): AtomicReference[jl.Boolean] =
                  new AtomicReference[jl.Boolean](null)
              })

          override def toString: String = delegate.toString

          def isDefined: Boolean =
            delegate.isDefined

          def apply(v1: Int): Boolean = {
            val value = delegate(v1)
            last.set(jl.Boolean.valueOf(value))
            value
          }

          def lastApply: Option[Boolean] = last.get match {
            case null => None
            case v => Some(v)
          }
        }
      }
    }
  }

  /**
   * A marker interface in support of [[components(ToggleMap)]]
   */
  private trait Composite {
    def components: Seq[ToggleMap]
  }

  /**
   * For some administrative purposes, it can be useful to get at the
   * component `ToggleMaps` that may make up a [[ToggleMap]].
   *
   * For example:
   * {{{
   * val toggleMap1: ToggleMap = ...
   * val toggleMap2: ToggleMap = ...
   * val combined = toggleMap1.orElse(toggleMap2)
   * assert(Seq(toggleMap1, toggleMap2) == ToggleMap.components(combined))
   * }}}
   */
  def components(toggleMap: ToggleMap): Seq[ToggleMap] = {
    toggleMap match {
      case composite: Composite =>
        composite.components.flatMap(components)
      case proxy: Proxy =>
        components(proxy.underlying)
      case _ =>
        Seq(toggleMap)
    }
  }

  /**
   * The [[ToggleMap]] interface is read only and this
   * is the mutable side of it.
   *
   * Implementations are expected to be thread-safe.
   */
  abstract class Mutable extends ToggleMap {

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
   * @note that inputs to [[Toggle.apply]] will be modified to promote
   *       better distributions in the face of low entropy inputs.
   *
   * @param id the name of the Toggle which is used to mix
   *           where along the universe of Ints does the range fall.
   * @param fraction the fraction, from 0.0 - 1.0 (inclusive), of Ints
   *          to return `true`. If outside of that range, a
   *          `java.lang.IllegalArgumentException` will be thrown.
   */
  private[toggle] def fractional(id: String, fraction: Double): Toggle = {
    Toggle.validateFraction(id, fraction)

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
      Toggle.off(id) // 0%
    } else if (start == end) {
      Toggle.on(id) // 100%
    } else if (start <= end) {
      // the range is contiguous without overflows.
      hashedToggle(id, { case i => i >= start && i <= end }, fraction)
    } else {
      // the range overflows around Int.MaxValue
      hashedToggle(id, { case i => i >= start || i <= end }, fraction)
    }
  }

  /**
   * Create a [[ToggleMap]] out of the given [[ToggleMap ToggleMaps]].
   *
   * If `toggleMaps` is empty, [[NullToggleMap]] will be returned.
   */
  @varargs
  def of(toggleMaps: ToggleMap*): ToggleMap = {
    val start: ToggleMap = NullToggleMap
    toggleMaps.foldLeft(start) {
      case (acc, tm) =>
        acc.orElse(tm)
    }
  }

  /**
   * A [[ToggleMap]] implementation based on immutable [[Toggle.Metadata]].
   */
  class Immutable(metadata: immutable.Seq[Toggle.Metadata]) extends ToggleMap {

    private[this] val toggles: immutable.Map[String, Toggle] =
      metadata.map { md => md.id -> fractional(md.id, md.fraction) }.toMap

    override def toString: String =
      s"ToggleMap.Immutable@${System.identityHashCode(this)}"

    def apply(id: String): Toggle =
      toggles.get(id) match {
        case Some(t) => t
        case None => Toggle.Undefined
      }

    def iterator: Iterator[Toggle.Metadata] =
      metadata.iterator
  }

  private[this] val log = Logger.get()

  private[this] val NoFractionAndToggle = (Double.NaN, Toggle.Undefined)

  private class MutableToggle(id: String) extends Toggle.Fractional(id) {
    private[this] val fractionAndToggle =
      new AtomicReference[(Double, Toggle)](NoFractionAndToggle)

    override def toString: String = s"MutableToggle($id)"

    def currentFraction: Double =
      fractionAndToggle.get()._1

    private[ToggleMap] def setFraction(fraction: Double): Unit = {
      val fAndT: (Double, Toggle) = if (Toggle.isValidFraction(fraction)) {
        (fraction, fractional(id, fraction))
      } else {
        NoFractionAndToggle
      }
      fractionAndToggle.set(fAndT)
    }

    def isDefined: Boolean =
      fractionAndToggle.get()._2.isDefined

    def apply(t: Int): Boolean =
      fractionAndToggle.get()._2(t)
  }

  /**
   * Create an empty [[Mutable]] instance with a default [[Metadata.source]]
   * specified.
   *
   * @note that inputs to [[Toggle.apply]] will be modified to promote
   *       better distributions in the face of low entropy inputs.
   */
  def newMutable(): Mutable =
    newMutable(None)

  /**
   * Create an empty [[Mutable]] instance with the given [[Metadata.source]].
   *
   * @note that inputs to [[Toggle.apply]] will be modified to promote
   *       better distributions in the face of low entropy inputs.
   */
  def newMutable(source: String): Mutable =
    newMutable(Some(source))

  private[this] def newMutable(source: Option[String]): Mutable = new Mutable {

    override def toString: String = source match {
      case Some(src) => src
      case None => s"ToggleMap.Mutable@${Integer.toHexString(hashCode())}"
    }

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

    def apply(id: String): Toggle =
      toggleFor(id)

    def iterator: Iterator[Toggle.Metadata] = {
      val source = toString
      toggles.asScala.collect {
        case (id, toggle) if Toggle.isValidFraction(toggle.currentFraction) =>
          Toggle.Metadata(id, toggle.currentFraction, None, source)
      }.toIterator
    }

    def put(id: String, fraction: Double): Unit = {
      if (Toggle.isValidFraction(fraction)) {
        log.info(s"Mutable Toggle id='$id' set to fraction=$fraction")
        toggleFor(id).setFraction(fraction)
      } else {
        log.warning(s"Mutable Toggle id='$id' ignoring invalid fraction=$fraction")
      }
    }

    def remove(id: String): Unit = {
      log.info(s"Mutable Toggle id='$id' removed")
      toggleFor(id).setFraction(Double.NaN)
    }
  }

  /**
   * A [[ToggleMap]] that is backed by a `com.twitter.app.GlobalFlag`,
   * [[flag.overrides]].
   *
   * Its [[Toggle Toggles]] will reflect changes to the underlying `Flag` which
   * enables usage in tests.
   *
   * Fractions that are out of range (outside of `[0.0-1.0]`) will be
   * ignored.
   *
   * @note that inputs to [[Toggle.apply]] will be modified to promote
   *       better distributions in the face of low entropy inputs.
   */
  val flags: ToggleMap = new ToggleMap {

    override def toString: String = "ToggleMap.Flags"

    private[this] def fractions: Map[String, Double] =
      flag.overrides()

    private[this] class FlagToggle(id: String) extends Toggle.Fractional(id) {
      private[this] val fractionAndToggle =
        new AtomicReference[(Double, Toggle)](NoFractionAndToggle)

      override def toString: String = s"FlagToggle($id)"

      def currentFraction: Double = fractionAndToggle.get()._1

      def isDefined: Boolean =
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

    def apply(id: String): Toggle =
      new FlagToggle(id)

    def iterator: Iterator[Toggle.Metadata] = {
      val source = toString
      fractions.iterator.collect {
        case (id, f) if Toggle.isValidFraction(f) =>
          Toggle.Metadata(id, f, None, source)
      }
    }
  }

  /**
   * A [[ToggleMap]] that proxies work to `underlying`.
   *
   * @note: Does not by itself denote that inheritors extends [[ToggleMap]], but
   *        can only be used by traits or classes that extend [[ToggleMap]].
   */
  trait Proxy { self: ToggleMap =>
    def underlying: ToggleMap

    override def toString: String = underlying.toString
    def apply(id: String): Toggle = underlying(id)
    def iterator: Iterator[Metadata] = underlying.iterator
  }

  private val MdDescFn: Toggle.Metadata => Option[String] =
    md => md.description

  /**
   * A [[ToggleMap]] which returns [[Toggle.on]] for all `ids`.
   *
   * @note [[ToggleMap.iterator]] will always be empty.
   */
  val On: ToggleMap = new ToggleMap {
    def apply(id: String): Toggle = Toggle.on(id)
    def iterator: Iterator[Metadata] = Iterator.empty
  }

  /**
   * A [[ToggleMap]] which returns [[Toggle.off]] for all `ids`.
   *
   * @note [[ToggleMap.iterator]] will always be empty.
   */
  val Off: ToggleMap = new ToggleMap {
    def apply(id: String): Toggle = Toggle.off(id)
    def iterator: Iterator[Metadata] = Iterator.empty
  }
}
