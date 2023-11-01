package com.twitter.finagle.mysql

import com.twitter.finagle.mysql.transport.MysqlBufWriter
import java.util.logging.Logger
import scala.language.implicitConversions

/**
 * A value of type `A` can implicitly convert to a `Parameter` if an evidence
 * `CanBeParameter[A]` is available in scope via the [[Parameter.wrap]] or
 * [[Parameter.wrapOption]] implicits. Explicit conversions are available
 * via [[Parameter.of]] and [[Parameters.of]].
 *
 * A Scala example with implicits:
 * {{{
 * import com.twitter.finagle.mysql.Parameter
 * import com.twitter.finagle.mysql.Parameter._
 *
 * val p: Parameter = "this will get implicitly converted to a Parameter"
 * }}}
 *
 * A Scala example without implicits:
 * {{{
 * import com.twitter.finagle.mysql.Parameter
 *
 * val p: Parameter = Parameter.of("explicitly converted to a Parameter")
 * }}}
 *
 * A Java example:
 * {{{
 * import com.twitter.finagle.mysql.Parameter;
 * import com.twitter.finagle.mysql.Parameters;
 *
 * Parameter p = Parameters.of("explicitly converted to a Parameter");
 * }}}
 */
sealed trait Parameter {
  type A
  def value: A
  def evidence: CanBeParameter[A]

  final def writeTo(writer: MysqlBufWriter): Unit = {
    evidence.write(writer, value)
  }

  final def size: Int = evidence.sizeOf(value)
  final def typeCode: Short = evidence.typeCode(value)
}

/**
 * For Scala users, the typical usage is by importing the implicit conversions
 * and then letting the compiler do the conversions for you. Explicit
 * runtime conversions are also available via [[Parameter.of]].
 *
 * Java users should generally be using [[Parameters.of]] to do explicit
 * conversions.
 *
 * A Scala example with implicits:
 * {{{
 * import com.twitter.finagle.mysql.Parameter
 * import com.twitter.finagle.mysql.Parameter._
 *
 * val p: Parameter = "this will get implicitly converted to a Parameter"
 * }}}
 *
 * A Scala example without implicits:
 * {{{
 * import com.twitter.finagle.mysql.Parameter
 *
 * val p: Parameter = Parameter.of("explicitly converted to a Parameter")
 * }}}
 *
 * A Java example:
 * {{{
 * import com.twitter.finagle.mysql.Parameter;
 * import com.twitter.finagle.mysql.Parameters;
 *
 * Parameter p = Parameters.of("explicitly converted to a Parameter");
 * }}}
 *
 * @see [[Parameters]] for a Java-friendly API.
 */
object Parameter {

  /**
   * Generally used via implicits.
   * Input of `None` is treated as a `NULL`.
   *
   * {{{
   * import com.twitter.finagle.mysql.Parameter
   * import com.twitter.finagle.mysql.Parameter._
   *
   * val p: Parameter = Some("this will get implicitly converted to a Parameter")
   * }}}
   *
   * Java users can use [[Parameters.of]].
   */
  implicit def wrapOption[_A](option: Option[_A])(implicit ev: CanBeParameter[_A]): Parameter =
    option match {
      case None => NullParameter
      case Some(value) => wrap(value)
    }

  /**
   * Generally used via implicits.
   *
   * {{{
   * import com.twitter.finagle.mysql.Parameter
   * import com.twitter.finagle.mysql.Parameter._
   *
   * val p: Parameter = "this will get implicitly converted to a Parameter"
   * }}}
   *
   * Java users can use [[Parameters.of]].
   */
  implicit def wrap[_A](_value: _A)(implicit _evidence: CanBeParameter[_A]): Parameter = {
    if (_value == null) {
      NullParameter
    } else {
      new Parameter {
        type A = _A
        def value: A = _value
        def evidence: CanBeParameter[A] = _evidence
        override def toString = s"Parameter($value)"
      }
    }
  }

  private val log = Logger.getLogger("finagle-mysql")

  /**
   * Converts the given `value` into a [[Parameter]] at runtime.
   *
   * Scala users may use the implicit conversions if they prefer.
   *
   * {{{
   * import com.twitter.finagle.mysql.Parameter
   *
   * val p: Parameter = Parameter.of("a string")
   * }}}
   *
   * @note Any unknown types are treated as a SQL NULL.
   * @see [[Parameters.of]] for Java users.
   */
  def of(value: Any): Parameter = value match {
    case null => NullParameter
    case Some(v) => of(v)
    case None => NullParameter
    case v: String => wrap(v)
    case v: Boolean => wrap(v)
    case v: Byte => wrap(v)
    case v: Short => wrap(v)
    case v: Int => wrap(v)
    case v: Long => wrap(v)
    case v: Float => wrap(v)
    case v: Double => wrap(v)
    case v: Array[Byte] => wrap(v)
    case v: Value => wrap(v)
    case v: java.sql.Timestamp => wrap(v)
    case v: java.sql.Date => wrap(v)
    case v: java.util.Date => wrap(v)
    case o: java.util.Optional[_] if o.isPresent => of(o.get())
    case o: java.util.Optional[_] => NullParameter
    case v =>
      // Unsupported type. Write the error to log, and write the type as null.
      // This allows us to safely skip writing the parameter without corrupting the buffer.
      log.warning(s"Unknown parameter ${v.getClass.getName} will be treated as SQL NULL.")
      NullParameter
  }

  /**
   * Synonym for [[of]].
   */
  def unsafeWrap(value: Any): Parameter = of(value)

  /**
   * Java users can use [[Parameters.nullParameter]].
   */
  object NullParameter extends Parameter {
    type A = Null
    def value: Null = null
    def evidence: CanBeParameter[Null] = CanBeParameter.nullCanBeParameter
    override def toString: String = "Parameter(null)"
  }
}

/**
 * A Java adaptation of the [[com.twitter.finagle.mysql.Parameter]] companion object.
 */
object Parameters {

  /**
   * See [[Parameter.NullParameter]].
   */
  def nullParameter: Parameter = Parameter.NullParameter

  /**
   * Java friendly factory method for a [[Parameter]].
   *
   * Synonym for [[of]].
   */
  def unsafeWrap(value: Any): Parameter = of(value)

  /**
   * Java friendly factory method for a [[Parameter]].
   */
  def of(value: Any): Parameter = Parameter.unsafeWrap(value)

  //TODO: create an accessor to Parameter.wrap, so type errors are caught at compile time.
}
