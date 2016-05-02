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
   * @param fraction must be between `0.0 and 1.0`, inclusive.
   *                 This represents the percentage of inputs that will
   *                 return `true`.
   * @param description an optional human-readable description of the Toggle's purpose.
   */
  case class Metadata(
      id: String,
      fraction: Double,
      description: Option[String]) {
    validateFraction(fraction)
  }

  private[toggle] def isValidFraction(f: Double): Boolean =
    f >= 0.0 && f <= 1.0

  private[toggle] def validateFraction(f: Double): Unit = {
    if (!isValidFraction(f))
      throw new IllegalArgumentException(
        s"fraction must be between 0.0 and 1.0 inclusive: $f")
  }

  private[toggle] def apply[T](
    string: String,
    pf: PartialFunction[T, Boolean]
  ): Toggle[T] = new Toggle[T] {
    override def toString: String = s"Toggle($string)"
    def isDefinedAt(x: T): Boolean = pf.isDefinedAt(x)
    def apply(v1: T): Boolean = pf(v1)
  }

  private[this] def constant[T](
    value: Boolean
  ): Toggle[T] =
    apply(value.toString, { case _ => value })

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
