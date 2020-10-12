package com.twitter.finagle.postgresql.types

// https://github.com/postgres/postgres/blob/24e2885ee304cb6a94fdfc25a1a108344ed9f4f7/src/include/catalog/pg_type.h#L68-L74
sealed trait Kind
object Kind {
  case object Base extends Kind
  case object Enum extends Kind
  case object Pseudo extends Kind
  case class Domain(inner: PgType) extends Kind
  case class Range(inner: PgType) extends Kind
  case class Array(inner: PgType) extends Kind
  case object Composite extends Kind
}
