package com.twitter.finagle.postgresql

import com.twitter.finagle.postgresql.types.PgType
import com.twitter.io.Buf

object Types {

  sealed trait Format
  object Format {
    final case object Text extends Format
    final case object Binary extends Format
  }

  final case class Oid(value: Long)
  final case class AttributeId(value: Short)

  final case class FieldDescription(
    name: String,
    tableOid: Option[Oid],
    tableAttributeId: Option[AttributeId],
    dataType: Oid,
    dataTypeSize: Short, // negative means variable length
    typeModifier: Int, // meaning is type-specific
    format: Format) {
    lazy val pgType: Option[PgType] = PgType.byOid(dataType)
  }

  // portal and statement naming
  sealed trait Name
  object Name {
    final case object Unnamed extends Name
    final case class Named(value: String) extends Name {
      require(value.nonEmpty, "named prepared statement cannot be empty")
    }
  }

  sealed trait WireValue
  object WireValue {
    final case object Null extends WireValue
    final case class Value(buf: Buf) extends WireValue
  }

  final case class PgArrayDim(size: Int, lowerBound: Int)
  final case class PgArray(
    dimensions: Int,
    dataOffset: Int, // 0 means no null values,
    elemType: Oid,
    arrayDims: IndexedSeq[PgArrayDim],
    data: IndexedSeq[WireValue])

  sealed trait Timestamp
  object Timestamp {
    final case object NegInfinity extends Timestamp
    final case object Infinity extends Timestamp
    final case class Micros(offset: Long) extends Timestamp
  }

  sealed trait NumericSign
  object NumericSign {
    final case object Positive extends NumericSign
    final case object Negative extends NumericSign
    final case object NaN extends NumericSign
    final case object Infinity extends NumericSign
    final case object NegInfinity extends NumericSign
  }
  final case class Numeric(
    weight: Short, // unsigned?
    sign: NumericSign,
    displayScale: Int, // unsigned short
    digits: Seq[Short] // NumericDigit
  )

  /**
   * Postgres Inet type wrapper.
   *
   * Postgres Inet type (https://www.postgresql.org/docs/current/datatype-net-types.html#DATATYPE-INET)
   * is a tuple made of an address and a subnet (or "network mask").
   *
   * @param ipAddress the IpAddress part, e.g.: 192.168.0.1
   * @param netmask the netmask, or number of bits to consider in `ipAddress`.
   *                This is 0 to 32 for IPv4 and 0 to 128 for IPv6.
   *                This is an unsigned byte value, so using a `Short`.
   */
  final case class Inet(ipAddress: java.net.InetAddress, netmask: Short)

}
