package com.twitter.finagle.exp.mysql

import com.twitter.finagle.exp.mysql.transport.BufferWriter
import language.implicitConversions

trait Parameter {
  type A
  def value: A
  def evidence: CanBeParameter[A]

  final def writeTo(writer: BufferWriter): Unit = {
    evidence.write(writer, value)
  }

  final def size = evidence.sizeOf(value)
  final def typeCode = evidence.typeCode(value)
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
      }
    }
  }

  object NullParameter extends Parameter {
    type A = Null
    def value = null
    def evidence = CanBeParameter.nullCanBeParameter
  }
}