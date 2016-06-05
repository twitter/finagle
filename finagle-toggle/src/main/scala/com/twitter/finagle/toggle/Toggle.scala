package com.twitter.finagle.toggle

/**
 * `Toggles` are used for modifying behavior without changing code.
 *
 * @see [[http://martinfowler.com/articles/feature-toggles.html Feature Toggles]]
 *      for detailed discussion on the topic.
 * @see [[ToggleMap]]
 */
abstract class Toggle[-T] extends PartialFunction[T, Boolean] { self =>

  /**
   * Similar to `PartialFunction.orElse` but specialized
   * for [[Toggle Toggles]].
   */
  def orElse[T1 <: T](that: Toggle[T1]): Toggle[T1] = {
    new Toggle[T1] {
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
   * @param fraction must be between `0.0 and 1.0`, inclusive.
   *                 This represents the percentage of inputs that will
   *                 return `true`.
   * @param description an optional human-readable description of the Toggle's purpose.
   */
  case class Metadata(
      id: String,
      fraction: Double,
      description: Option[String]) {
    validateId(id)
    validateFraction(fraction)
  }

  private[toggle] def isValidFraction(f: Double): Boolean =
    f >= 0.0 && f <= 1.0

  private[toggle] def validateFraction(f: Double): Unit = {
    if (!isValidFraction(f))
      throw new IllegalArgumentException(
        s"fraction must be between 0.0 and 1.0 inclusive: $f")
  }

  private[this] val AllowedIdChars: Set[Char] = {
    ('A'.to('Z') ++
      'a'.to('z') ++
      '0'.to('9') ++
      Set('_', '-', '.')).toSet
  }

  private[toggle] def validateId(id: String): Unit = {
    val invalidCh = id.find { ch =>
      !AllowedIdChars.contains(ch)
    }
    invalidCh match {
      case Some(ch) =>
        throw new IllegalArgumentException(s"invalid char '$ch' in id: '$id'")
      case None =>
    }

    // do some minimal verification to make sure it looks "packagey".
    if (id.length < 3)
      throw new IllegalArgumentException(s"id too short: '$id'")
    // test that it has atleast 1 "."
    val firstDot = id.indexOf('.')
    if (firstDot <= 0)
      throw new IllegalArgumentException(s"id must be package-like: '$id'")
  }

  private[toggle] def apply[T](
    string: String,
    pf: PartialFunction[T, Boolean]
  ): Toggle[T] = new Toggle[T] {
    Toggle.validateId(string)
    override def toString: String = s"Toggle($string)"
    def isDefinedAt(x: T): Boolean = pf.isDefinedAt(x)
    def apply(v1: T): Boolean = pf(v1)
  }

  private[this] def constant[T](
    value: Boolean
  ): Toggle[T] =
    apply("com.twitter.finagle.toggle." + value.toString, { case _ => value })

  /**
   * A [[Toggle]] which is defined for all inputs and always returns `true`.
   */
  val True: Toggle[Any] = constant(true)

  /**
   * A [[Toggle]] which is defined for all inputs and always returns `false`.
   */
  val False: Toggle[Any] = constant(false)

  /**
   * A [[Toggle]] which is defined for no inputs.
   */
  private[toggle] val Undefined: Toggle[Any] =
    new Toggle[Any] {
      def isDefinedAt(x: Any): Boolean = false
      def apply(v1: Any): Boolean = throw new UnsupportedOperationException()
      override def toString: String = "Undefined"
    }

}
