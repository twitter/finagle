package com.twitter.finagle.mysql

import com.twitter.finagle.mysql.transport.{MysqlBuf, MysqlBufWriter}
import java.{lang => jl}

trait CanBeParameter[-A] {

  /**
   * Returns the size of the given parameter in its MySQL binary representation.
   */
  def sizeOf(param: A): Int

  /**
   * Returns the MySQL type code for the given parameter.
   */
  def typeCode(param: A): Short

  def write(writer: MysqlBufWriter, param: A): Unit
}

/**
 * When a new implicit [[CanBeParameter]] is added here, it should also be
 * explicitly added to [[Parameter.unsafeWrap]].
 */
object CanBeParameter {
  private[this] def arrayLength(bytes: Array[Byte]): Int =
    MysqlBuf.sizeOfLen(bytes.length) + bytes.length

  implicit val stringCanBeParameter: CanBeParameter[String] = {
    new CanBeParameter[String] {
      def sizeOf(param: String): Int = {
        val bytes = param.getBytes(MysqlCharset.defaultCharset)
        arrayLength(bytes)
      }

      def typeCode(param: String): Short = Type.VarChar
      def write(writer: MysqlBufWriter, param: String): Unit =
        writer.writeLengthCodedString(param, MysqlCharset.defaultCharset)
    }
  }

  implicit val booleanCanBeParameter: CanBeParameter[Boolean] = {
    new CanBeParameter[Boolean] {
      def sizeOf(param: Boolean): Int = 1
      def typeCode(param: Boolean): Short = Type.Tiny
      def write(writer: MysqlBufWriter, param: Boolean): Unit =
        writer.writeByte(if (param) 1 else 0)
    }
  }

  implicit val javaLangBooleanCanBeParameter: CanBeParameter[jl.Boolean] = {
    new CanBeParameter[jl.Boolean] {
      def sizeOf(param: jl.Boolean): Int = booleanCanBeParameter.sizeOf(param.booleanValue())
      def typeCode(param: jl.Boolean): Short = booleanCanBeParameter.typeCode(param.booleanValue())
      def write(writer: MysqlBufWriter, param: jl.Boolean): Unit =
        booleanCanBeParameter.write(writer, param.booleanValue())
    }
  }

  implicit val byteCanBeParameter: CanBeParameter[Byte] = {
    new CanBeParameter[Byte] {
      def sizeOf(param: Byte): Int = 1
      def typeCode(param: Byte): Short = Type.Tiny
      def write(writer: MysqlBufWriter, param: Byte): Unit =
        writer.writeByte(param)
    }
  }

  implicit val javaLangByteCanBeParameter: CanBeParameter[jl.Byte] = {
    new CanBeParameter[jl.Byte] {
      def sizeOf(param: jl.Byte): Int = byteCanBeParameter.sizeOf(param.byteValue())
      def typeCode(param: jl.Byte): Short = byteCanBeParameter.typeCode(param.byteValue())
      def write(writer: MysqlBufWriter, param: jl.Byte): Unit =
        byteCanBeParameter.write(writer, param.byteValue())
    }
  }

  implicit val shortCanBeParameter: CanBeParameter[Short] = {
    new CanBeParameter[Short] {
      def sizeOf(param: Short): Int = 2
      def typeCode(param: Short): Short = Type.Short
      def write(writer: MysqlBufWriter, param: Short): Unit =
        writer.writeShortLE(param)
    }
  }

  implicit val javaLangShortCanBeParameter: CanBeParameter[jl.Short] = {
    new CanBeParameter[jl.Short] {
      def sizeOf(param: jl.Short): Int = shortCanBeParameter.sizeOf(param.shortValue())
      def typeCode(param: jl.Short): Short = shortCanBeParameter.typeCode(param.shortValue())
      def write(writer: MysqlBufWriter, param: jl.Short): Unit =
        shortCanBeParameter.write(writer, param.shortValue())
    }
  }

  implicit val intCanBeParameter: CanBeParameter[Int] = {
    new CanBeParameter[Int] {
      def sizeOf(param: Int): Int = 4
      def typeCode(param: Int): Short = Type.Long
      def write(writer: MysqlBufWriter, param: Int): Unit =
        writer.writeIntLE(param)
    }
  }

  implicit val javaLangIntCanBeParameter: CanBeParameter[jl.Integer] = {
    new CanBeParameter[jl.Integer] {
      def sizeOf(param: jl.Integer): Int = intCanBeParameter.sizeOf(param.intValue())
      def typeCode(param: jl.Integer): Short = intCanBeParameter.typeCode(param.intValue())
      def write(writer: MysqlBufWriter, param: jl.Integer): Unit =
        intCanBeParameter.write(writer, param.intValue())
    }
  }

  implicit val longCanBeParameter: CanBeParameter[Long] = {
    new CanBeParameter[Long] {
      def sizeOf(param: Long): Int = 8
      def typeCode(param: Long): Short = Type.LongLong
      def write(writer: MysqlBufWriter, param: Long): Unit =
        writer.writeLongLE(param)
    }
  }

  implicit val javaLangLongCanBeParameter: CanBeParameter[jl.Long] = {
    new CanBeParameter[jl.Long] {
      def sizeOf(param: jl.Long): Int = longCanBeParameter.sizeOf(param.longValue())
      def typeCode(param: jl.Long): Short = longCanBeParameter.typeCode(param.longValue())
      def write(writer: MysqlBufWriter, param: jl.Long): Unit =
        longCanBeParameter.write(writer, param.longValue())
    }
  }

  implicit val bigIntCanBeParameter: CanBeParameter[BigInt] = {
    new CanBeParameter[BigInt] {
      def sizeOf(param: BigInt): Int = 8
      def typeCode(param: BigInt): Short = Type.LongLong
      def write(writer: MysqlBufWriter, param: BigInt): Unit = {
        val byteArray: Array[Byte] = param.toByteArray
        val lengthOfByteArray: Int = byteArray.length

        if (lengthOfByteArray > 8) {
          throw new BigIntTooLongException(size = lengthOfByteArray)
        }

        for (i <- (lengthOfByteArray - 1) to 0 by -1) {
          writer.writeByte(byteArray(i))
        }

        for (i <- lengthOfByteArray until 8) {
          writer.writeByte(0x0)
        }
      }
    }
  }

  implicit val floatCanBeParameter: CanBeParameter[Float] = {
    new CanBeParameter[Float] {
      def sizeOf(param: Float): Int = 4
      def typeCode(param: Float): Short = Type.Float
      def write(writer: MysqlBufWriter, param: Float): Unit =
        writer.writeFloatLE(param)
    }
  }

  implicit val javaLangFloatCanBeParameter: CanBeParameter[jl.Float] = {
    new CanBeParameter[jl.Float] {
      def sizeOf(param: jl.Float): Int = floatCanBeParameter.sizeOf(param.floatValue())
      def typeCode(param: jl.Float): Short = floatCanBeParameter.typeCode(param.floatValue())
      def write(writer: MysqlBufWriter, param: jl.Float): Unit =
        floatCanBeParameter.write(writer, param.floatValue())
    }
  }

  implicit val doubleCanBeParameter: CanBeParameter[Double] = {
    new CanBeParameter[Double] {
      def sizeOf(param: Double): Int = 8
      def typeCode(param: Double): Short = Type.Double
      def write(writer: MysqlBufWriter, param: Double): Unit =
        writer.writeDoubleLE(param)
    }
  }

  implicit val javaLangDoubleCanBeParameter: CanBeParameter[jl.Double] = {
    new CanBeParameter[jl.Double] {
      def sizeOf(param: jl.Double): Int = doubleCanBeParameter.sizeOf(param.doubleValue())
      def typeCode(param: jl.Double): Short = doubleCanBeParameter.typeCode(param.doubleValue())
      def write(writer: MysqlBufWriter, param: jl.Double): Unit =
        doubleCanBeParameter.write(writer, param.doubleValue())
    }
  }

  // the format: varlen followed by the value as a string
  // https://dev.mysql.com/doc/internals/en/binary-protocol-value.html#packet-ProtocolBinary::MYSQL_TYPE_NEWDECIMAL
  implicit val bigDecimalCanBeParameter: CanBeParameter[BigDecimal] =
    new CanBeParameter[BigDecimal] {
      private[this] val binaryCharset = MysqlCharset(MysqlCharset.Binary)

      private[this] def asBytes(bd: BigDecimal): Array[Byte] =
        bd.toString.getBytes(binaryCharset)

      def sizeOf(param: BigDecimal): Int =
        arrayLength(asBytes(param))

      def typeCode(param: BigDecimal): Short =
        Type.NewDecimal

      def write(writer: MysqlBufWriter, param: BigDecimal): Unit =
        writer.writeLengthCodedBytes(asBytes(param))
    }

  implicit val byteArrayCanBeParameter: CanBeParameter[Array[Byte]] = {
    new CanBeParameter[Array[Byte]] {
      def sizeOf(param: Array[Byte]): Int =
        arrayLength(param)

      def typeCode(param: Array[Byte]): Short = {
        if (param.length <= 255) Type.TinyBlob
        else if (param.length <= 65535) Type.Blob
        else if (param.length <= 16777215) Type.MediumBlob
        else -1
      }
      def write(writer: MysqlBufWriter, param: Array[Byte]): Unit =
        writer.writeLengthCodedBytes(param)
    }
  }

  implicit val valueCanBeParameter: CanBeParameter[Value] = {
    new CanBeParameter[Value] {
      def sizeOf(param: Value): Int = param match {
        case RawValue(_, _, true, b) => arrayLength(b)
        case StringValue(s) =>
          val bytes = s.getBytes(MysqlCharset.defaultCharset)
          arrayLength(bytes)
        case ByteValue(_) => 1
        case ShortValue(_) => 2
        case IntValue(_) => 4
        case LongValue(_) => 8
        case BigIntValue(_) => 8
        case FloatValue(_) => 4
        case DoubleValue(_) => 8
        case NullValue => 0
        case _ => throw new IllegalArgumentException(s"Cannot determine size of $param.")
      }

      def typeCode(param: Value): Short = param match {
        case RawValue(typ, _, _, _) => typ
        case StringValue(_) => Type.VarChar
        case ByteValue(_) => Type.Tiny
        case ShortValue(_) => Type.Short
        case IntValue(_) => Type.Long
        case LongValue(_) => Type.LongLong
        case BigIntValue(_) => Type.LongLong
        case FloatValue(_) => Type.Float
        case DoubleValue(_) => Type.Double
        case EmptyValue => -1
        case NullValue => Type.Null
      }

      def write(writer: MysqlBufWriter, param: Value): Unit = param match {
        // allows for generic binary values as params to a prepared statement.
        case RawValue(_, _, true, bytes) => writer.writeLengthCodedBytes(bytes)
        // allows for Value types as params to prepared statements
        case ByteValue(b) => writer.writeByte(b)
        case ShortValue(s) => writer.writeShortLE(s)
        case IntValue(i) => writer.writeIntLE(i)
        case LongValue(l) => writer.writeLongLE(l)
        case BigIntValue(b) => bigIntCanBeParameter.write(writer, b)
        case FloatValue(f) => writer.writeFloatLE(f)
        case DoubleValue(d) => writer.writeDoubleLE(d)
        case StringValue(s) => writer.writeLengthCodedString(s, MysqlCharset.defaultCharset)
        case _ =>
          throw new IllegalArgumentException(s"Type $param is not supported, cannot write value.")
      }
    }
  }

  /**
   * Because java.sql.Date and java.sql.Timestamp extend java.util.Date and
   * because CanBeParameter's type parameter is contravariant, having separate
   * implicits for these types results in the one for the supertype being used
   * when the one for the subtype should be used. To work around this we use
   * just one implicit and pattern match within it.
   */
  implicit val dateCanBeParameter: CanBeParameter[java.util.Date] = {
    new CanBeParameter[java.util.Date] {
      def sizeOf(param: java.util.Date): Int = param match {
        case _: java.sql.Date => 5
        case _: java.sql.Timestamp => 12
        case _ => 12
      }

      def typeCode(param: java.util.Date): Short = param match {
        case _: java.sql.Date => Type.Date
        case _: java.sql.Timestamp => Type.Timestamp
        case _ => Type.DateTime
      }

      def write(writer: MysqlBufWriter, param: java.util.Date): Unit = param match {
        case sqlDate: java.sql.Date => valueCanBeParameter.write(writer, DateValue(sqlDate))
        case sqlTimestamp: java.sql.Timestamp =>
          valueCanBeParameter.write(
            writer,
            TimestampValue(sqlTimestamp)
          )
        case javaDate =>
          valueCanBeParameter.write(
            writer,
            TimestampValue(new java.sql.Timestamp(javaDate.getTime))
          )
      }
    }
  }

  // Note that Timestamp is UTC only and includes both Date and Time parts.
  // See https://dev.mysql.com/doc/refman/8.0/en/datetime.html.
  implicit val ctuTimeCanBeParameter: CanBeParameter[com.twitter.util.Time] = {
    new CanBeParameter[com.twitter.util.Time] {
      def sizeOf(param: com.twitter.util.Time): Int = 12
      def typeCode(param: com.twitter.util.Time): Short = Type.Timestamp
      def write(writer: MysqlBufWriter, param: com.twitter.util.Time): Unit = {
        valueCanBeParameter.write(writer, TimestampValue(new java.sql.Timestamp(param.inMillis)))
      }
    }
  }

  implicit val nullCanBeParameter: CanBeParameter[Null] = {
    new CanBeParameter[Null] {
      def sizeOf(param: Null): Int = 0
      def typeCode(param: Null): Short = Type.Null
      def write(writer: MysqlBufWriter, param: Null): Unit = ()
    }
  }
}

class BigIntTooLongException(size: Int)
    extends Exception(
      s"BigInt is stored as Unsigned Long, thus it cannot be longer than 8 bytes. Size = $size"
    )
