package com.twitter.finagle.exp.mysql

import com.twitter.finagle.exp.mysql.transport.BufferWriter
import java.util.logging.Logger
import language.implicitConversions

/**
 * A value of type `A` can implicitly convert to a `Parameter` if an evidence `CanBeParameter[A]` is
 * available in scope. This type is not to be instantiated in any other manner.
 */
sealed trait Parameter {
  type A
  def value: A
  def evidence: CanBeParameter[A]

  final def writeTo(writer: BufferWriter): Unit = {
    evidence.write(writer, value)
  }

  final def size: Int = evidence.sizeOf(value)
  final def typeCode: Short = evidence.typeCode(value)
}

object Parameter {
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
   * This converts the compile time error we would get with `wrap` into
   * a run time error. This method should only be used to ease migration
   * from Any to Parameter. It maintains the previous behavior where
   * we log a failure to encode and transparently write a SQL NULL to
   * the wire.
   */
  def unsafeWrap(value: Any): Parameter = value match {
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
    case null => Parameter.NullParameter
    case v =>
      // Unsupported type. Write the error to log, and write the type as null.
      // This allows us to safely skip writing the parameter without corrupting the buffer.
      log.warning(s"Unknown parameter ${v.getClass.getName} will be treated as SQL NULL.")
      Parameter.NullParameter
  }

  object NullParameter extends Parameter {
    type A = Null
    def value = null
    def evidence = CanBeParameter.nullCanBeParameter
  }
}