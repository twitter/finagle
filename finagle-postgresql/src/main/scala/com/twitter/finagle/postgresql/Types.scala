package com.twitter.finagle.postgresql

import com.twitter.io.Buf

object Types {

  sealed trait Format
  object Format {
    case object Text extends Format
    case object Binary extends Format
  }

  case class Oid(value: Int)
  case class AttributeId(value: Int)

  case class FieldDescription(
    name: String,
    tableOid: Option[Oid],
    tableAttributeId: Option[AttributeId],
    dataType: Oid,
    dataTypeSize: Short, // negative means variable length
    typeModifier: Int, // meaning is type-specific
    format: Format
  )

  // portal and statement naming
  sealed trait Name
  object Name {
    case object Unnamed extends Name
    case class Named(value: String) extends Name {
      require(value.length > 0, "named prepared statement cannot be empty")
    }
  }

  sealed trait WireValue
  object WireValue {
    case object Null extends WireValue
    case class Value(buf: Buf) extends WireValue
  }

}
