package com.twitter.finagle.toggle

/**
 * `Toggles` are used for modifying behavior without changing code.
 *
 * @param id the identifying name of the `Toggle`.
 *           These should generally be fully qualified names to avoid conflicts
 *           between libraries. For example, "com.twitter.finagle.CoolThing".
 *           Valid characters are `A-Z`, `a-z`, `0-9`, `_`, `-`, `.`.
 *
 * @see [[http://martinfowler.com/articles/feature-toggles.html Feature Toggles]]
 *      for detailed discussion on the topic.
 * @see [[ToggleMap]]
 */
abstract class Toggle[-T](
    private[toggle] val id: String)
  extends PartialFunction[T, Boolean] { self =>

  Toggle.validateId(id)

  /**
   * Similar to `PartialFunction.orElse` but specialized
   * for [[Toggle Toggles]].
   *
   * @note the returned [[Toggle]] will keep the current `id`.
   */
  def orElse[T1 <: T](that: Toggle[T1]): Toggle[T1] = {
    new Toggle[T1](self.id) {
      override def toString: String =
        s"${self.toString}.orElse(${that.toString})"

      def isDefinedAt(x: T1): Boolean =
        self.isDefinedAt(x) || that.isDefinedAt(x)

      def apply(v1: T1): Boolean =
        self.applyOrElse(v1, that)
    }
  }
}

object Toggle {

  /**
   * The metadata about a [[Toggle]].
   *
   * @param id the identifying name of the `Toggle`.
   *           These should generally be fully qualified names to avoid conflicts
   *           between libraries. For example, "com.twitter.finagle.CoolThing".
   *           Valid characters are `A-Z`, `a-z`, `0-9`, `_`, `-`, `.`.
   *           See [[Toggle$.isValidId]].
   * @param fraction must be between `0.0 and 1.0`, inclusive.
   *                 This represents the fraction of inputs that will
   *                 return `true`. See [[Toggle$.isValidFraction]].
   * @param description human-readable description of the Toggle's purpose.
   * @param source the origin of this [[Toggle]] which is often given by
   *               `toString` of the [[ToggleMap]] that created it.
   */
  case class Metadata(
      id: String,
      fraction: Double,
      description: Option[String],
      source: String) {
    validateId(id)
    validateFraction(id, fraction)
    validateDescription(id, description)
  }

  /**
   * A mixin to get access to the last result of [[Toggle.apply]].
   *
   * Useful for providing visibility to how a toggle is
   * being used at runtime.
   *
   * @see [[ToggleMap.observed]]
   */
  trait Captured {
    /**
     * `None` if no calls to [[Toggle.apply]] have been made.
     */
    def lastApply: Option[Boolean]
  }

  /**
   * Whether or not the given `fraction` is valid.
   *
   * @param fraction must be between `0.0 and 1.0`, inclusive.
   *                 This represents the fraction of inputs that will
   *                 return `true`.
   */
  def isValidFraction(fraction: Double): Boolean =
    fraction >= 0.0 && fraction <= 1.0

  private[toggle] def validateFraction(id: String, f: Double): Unit = {
    if (!isValidFraction(f))
      throw new IllegalArgumentException(
        s"fraction for $id must be between 0.0 and 1.0 inclusive: $f")
  }

  private[this] def validateDescription(id: String, desc: Option[String]): Unit = {
    desc match {
      case None =>
      case Some(d) =>
        if (d.trim.isEmpty)
          throw new IllegalArgumentException(
            s"description for $id must not be empty: '$d'")
    }
  }

  private[this] val AllowedIdChars: Set[Char] = {
    ('A'.to('Z') ++
      'a'.to('z') ++
      '0'.to('9') ++
      Set('_', '-', '.')).toSet
  }

  /** Return `Some(ErrorMessage)` when invalid. */
  private def checkId(id: String): Option[String] = {
    val invalidCh = id.find { ch =>
      !AllowedIdChars.contains(ch)
    }
    invalidCh match {
      case Some(ch) =>
        Some(s"invalid char '$ch' in id: '$id'")
      case None =>
        // do some minimal verification to make sure it looks "packagey".
        if (id.length < 3) {
          Some(s"id too short: '$id'")
        } else {
          // test that it has atleast 1 "."
          val firstDot = id.indexOf('.')
          if (firstDot <= 0)
            Some(s"id must be package-like: '$id'")
          else
            None
        }
    }
  }

  /**
   * Whether or not the given `id` is valid.
   *
   * @param id the identifying name of the `Toggle`.
   *           These should generally be fully qualified names to avoid conflicts
   *           between libraries. For example, "com.twitter.finagle.CoolThing".
   *           Valid characters are `A-Z`, `a-z`, `0-9`, `_`, `-`, `.`.
   */
  def isValidId(id: String): Boolean =
    checkId(id).isEmpty

  private[toggle] def validateId(id: String): Unit = {
    checkId(id) match {
      case Some(msg) => throw new IllegalArgumentException(msg)
      case None =>
    }
  }

  private def apply[T](
    id: String,
    pf: PartialFunction[T, Boolean]
  ): Toggle[T] = new Toggle[T](id) {
    override def toString: String = s"Toggle($id)"
    def isDefinedAt(x: T): Boolean = pf.isDefinedAt(x)
    def apply(v1: T): Boolean = pf(v1)
  }

  private[this] val AlwaysTrue: PartialFunction[Any, Boolean] =
    { case _ => true }

  /**
   * A [[Toggle]] which is defined for all inputs and always returns `true`.
   */
  def on[T](id: String): Toggle[T] =
    apply(id, AlwaysTrue)

  private[this] val AlwaysFalse: PartialFunction[Any, Boolean] =
    { case _ => false }

  /**
   * A [[Toggle]] which is defined for all inputs and always returns `false`.
   */
  def off[T](id: String): Toggle[T] =
    apply(id, AlwaysFalse)

  /**
   * A [[Toggle]] which is defined for no inputs.
   */
  private[toggle] val Undefined: Toggle[Any] =
    new Toggle[Any]("com.twitter.finagle.toggle.Undefined") {
      def isDefinedAt(x: Any): Boolean = false
      def apply(v1: Any): Boolean = throw new UnsupportedOperationException()
      override def toString: String = "Undefined"

      // an optimization that allows for avoiding unnecessary Toggles
      // by "flattening" them out.
      override def orElse[T](that: Toggle[T]): Toggle[T] = that
    }

}
