package com.twitter.finagle.exp.mysql

import com.twitter.finagle.exp.mysql.transport.BufferWriter
import language.implicitConversions

/**
 * A value of type `A` can implicitly convert to a `Parameter` if an evidence `CanBeParameter[A]` is
 * available in scope. This type is not be instantiated in any other manner.
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
  implicit def wrap[A0](value0: A0)(implicit evidence0: CanBeParameter[A0]): Parameter = {
    if (value0 == null) {
      NullParameter
    } else {
      new Parameter {
        type A = A0
        def value: A = value0
        def evidence: CanBeParameter[A] = evidence0
      }
    }
  }

  object NullParameter extends Parameter {
    type A = Null
    def value = null
    def evidence = CanBeParameter.nullCanBeParameter
  }
}