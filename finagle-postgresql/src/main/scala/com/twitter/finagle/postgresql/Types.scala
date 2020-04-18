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

  // portal and statement naming
  sealed trait Name
  object Name {
    case object Unnamed extends Name
    case class Named(value: String) extends Name
  }

  sealed trait WireValue
  object WireValue {
    case object Null extends WireValue
    case class Value(buf: Buf) extends WireValue
  }

}
