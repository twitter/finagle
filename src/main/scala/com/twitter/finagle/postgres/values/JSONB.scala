package com.twitter.finagle.postgres.values

object JSONB {
  def apply(string: String): JSONB = new JSONB(string.getBytes())

  def stringify(jsonb: JSONB): String = new String(jsonb.bytes)
}

case class JSONB(bytes: Array[Byte]) extends AnyVal

