package com.twitter.finagle.mysql

import com.twitter.finagle.mysql.transport.{MysqlBuf, MysqlBufWriter}

trait CanBeParameter[-A] { outer =>

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

object CanBeParameter {
  implicit val stringCanBeParameter: CanBeParameter[String] = {
    new CanBeParameter[String] {
      def sizeOf(param: String): Int = {
        val bytes = param.getBytes(Charset.defaultCharset)
        MysqlBuf.sizeOfLen(bytes.size) + bytes.size
      }

      def typeCode(param: String): Short = Type.VarChar
      def write(writer: MysqlBufWriter, param: String): Unit =
        writer.writeLengthCodedString(param, Charset.defaultCharset)
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

  implicit val byteCanBeParameter: CanBeParameter[Byte] = {
    new CanBeParameter[Byte] {
      def sizeOf(param: Byte): Int = 1
      def typeCode(param: Byte): Short = Type.Tiny
      def write(writer: MysqlBufWriter, param: Byte): Unit =
        writer.writeByte(param)
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

  implicit val intCanBeParameter: CanBeParameter[Int] = {
    new CanBeParameter[Int] {
      def sizeOf(param: Int): Int = 4
      def typeCode(param: Int): Short = Type.Long
      def write(writer: MysqlBufWriter, param: Int): Unit =
        writer.writeIntLE(param)
    }
  }

  implicit val longCanBeParameter = {
    new CanBeParameter[Long] {
      def sizeOf(param: Long): Int = 8
      def typeCode(param: Long): Short = Type.LongLong
      def write(writer: MysqlBufWriter, param: Long): Unit =
        writer.writeLongLE(param)
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

  implicit val doubleCanBeParameter: CanBeParameter[Double] = {
    new CanBeParameter[Double] {
      def sizeOf(param: Double): Int = 8
      def typeCode(param: Double): Short = Type.Double
      def write(writer: MysqlBufWriter, param: Double): Unit =
        writer.writeDoubleLE(param)
    }
  }

  implicit val byteArrayCanBeParameter: CanBeParameter[Array[Byte]] = {
    new CanBeParameter[Array[Byte]] {
      def sizeOf(param: Array[Byte]): Int = MysqlBuf.sizeOfLen(param.length) + param.length
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
        case RawValue(_, _, true, b) => MysqlBuf.sizeOfLen(b.length) + b.length
        case StringValue(s) =>
          val bytes = s.getBytes(Charset.defaultCharset);
          MysqlBuf.sizeOfLen(bytes.length) + bytes.length
        case ByteValue(_) => 1
        case ShortValue(_) => 2
        case IntValue(_) => 4
        case LongValue(_) => 8
        case BigIntValue(_) => 8
        case FloatValue(_) => 4
        case DoubleValue(_) => 8
        case NullValue => 0
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
        case StringValue(s) => writer.writeLengthCodedString(s, Charset.defaultCharset)
      }
    }
  }

  implicit val timestampCanBeParameter: CanBeParameter[java.sql.Timestamp] = {
    new CanBeParameter[java.sql.Timestamp] {
      def sizeOf(param: java.sql.Timestamp): Int = 12
      def typeCode(param: java.sql.Timestamp): Short = Type.Timestamp
      def write(writer: MysqlBufWriter, param: java.sql.Timestamp): Unit = {
        valueCanBeParameter.write(writer, TimestampValue(param))
      }
    }
  }

  implicit val sqlDateCanBeParameter: CanBeParameter[java.sql.Date] = {
    new CanBeParameter[java.sql.Date] {
      def sizeOf(param: java.sql.Date): Int = 5
      def typeCode(param: java.sql.Date): Short = Type.Date
      def write(writer: MysqlBufWriter, param: java.sql.Date): Unit = {
        valueCanBeParameter.write(writer, DateValue(param))
      }
    }
  }

  implicit val javaDateCanBeParameter: CanBeParameter[java.util.Date] = {
    new CanBeParameter[java.util.Date] {
      def sizeOf(param: java.util.Date): Int = 12
      def typeCode(param: java.util.Date): Short = Type.DateTime
      def write(writer: MysqlBufWriter, param: java.util.Date): Unit = {
        valueCanBeParameter.write(writer, TimestampValue(new java.sql.Timestamp(param.getTime)))
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
