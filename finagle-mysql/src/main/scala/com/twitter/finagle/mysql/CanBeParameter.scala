package com.twitter.finagle.exp.mysql

import com.twitter.finagle.exp.mysql.transport.{MysqlBuf, MysqlBufWriter}

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
  implicit val stringCanBeParameter = {
    new CanBeParameter[String] {
      def sizeOf(param: String): Int = {
        val bytes = param.getBytes(Charset.defaultCharset)
        MysqlBuf.sizeOfLen(bytes.size) + bytes.size
      }

      def typeCode(param: String) = Type.VarChar
      def write(writer: MysqlBufWriter, param: String) = {
        writer.writeLengthCodedString(param, Charset.defaultCharset)
      }
    }
  }

  implicit val booleanCanBeParameter = {
    new CanBeParameter[Boolean] {
      def sizeOf(param: Boolean) = 1
      def typeCode(param: Boolean) = Type.Tiny
      def write(writer: MysqlBufWriter, param: Boolean) = writer.writeByte(if (param) 1 else 0)
    }
  }

  implicit val byteCanBeParameter = {
    new CanBeParameter[Byte] {
      def sizeOf(param: Byte) = 1
      def typeCode(param: Byte) = Type.Tiny
      def write(writer: MysqlBufWriter, param: Byte) = writer.writeByte(param)
    }
  }

  implicit val shortCanBeParameter = {
    new CanBeParameter[Short] {
      def sizeOf(param: Short) = 2
      def typeCode(param: Short) = Type.Short
      def write(writer: MysqlBufWriter, param: Short) = writer.writeShortLE(param)
    }
  }

  implicit val intCanBeParameter = {
    new CanBeParameter[Int] {
      def sizeOf(param: Int) = 4
      def typeCode(param: Int) = Type.Long
      def write(writer: MysqlBufWriter, param: Int) = writer.writeIntLE(param)
    }
  }

  implicit val longCanBeParameter = {
    new CanBeParameter[Long] {
      def sizeOf(param: Long) = 8
      def typeCode(param: Long) = Type.LongLong
      def write(writer: MysqlBufWriter, param: Long) = writer.writeLongLE(param)
    }
  }

  implicit val floatCanBeParameter = {
    new CanBeParameter[Float] {
      def sizeOf(param: Float) = 4
      def typeCode(param: Float) = Type.Float
      def write(writer: MysqlBufWriter, param: Float) = writer.writeFloatLE(param)
    }
  }

  implicit val doubleCanBeParameter = {
    new CanBeParameter[Double] {
      def sizeOf(param: Double) = 8
      def typeCode(param: Double) = Type.Double
      def write(writer: MysqlBufWriter, param: Double) = writer.writeDoubleLE(param)
    }
  }

  implicit val byteArrayCanBeParameter = {
    new CanBeParameter[Array[Byte]] {
      def sizeOf(param: Array[Byte]) = MysqlBuf.sizeOfLen(param.length) + param.length
      def typeCode(param: Array[Byte]) = {
        if (param.length <= 255) Type.TinyBlob
        else if (param.length <= 65535) Type.Blob
        else if (param.length <= 16777215) Type.MediumBlob
        else -1
      }
      def write(writer: MysqlBufWriter, param: Array[Byte]) = writer.writeLengthCodedBytes(param)
    }
  }

  implicit val valueCanBeParameter = {
    new CanBeParameter[Value] {
      def sizeOf(param: Value) = param match {
        case RawValue(_, _, true, b) => MysqlBuf.sizeOfLen(b.length) + b.length
        case StringValue(s)          => val bytes = s.getBytes(Charset.defaultCharset); MysqlBuf.sizeOfLen(bytes.length) + bytes.length
        case ByteValue(_)            => 1
        case ShortValue(_)           => 2
        case IntValue(_)             => 4
        case LongValue(_)            => 8
        case FloatValue(_)           => 4
        case DoubleValue(_)          => 8
        case NullValue               => 0
        case _                       => 0
      }

      def typeCode(param: Value) = param match {
        case RawValue(typ, _, _, _) => typ
        case StringValue(_)         => Type.VarChar
        case ByteValue(_)           => Type.Tiny
        case ShortValue(_)          => Type.Short
        case IntValue(_)            => Type.Long
        case LongValue(_)           => Type.LongLong
        case FloatValue(_)          => Type.Float
        case DoubleValue(_)         => Type.Double
        case EmptyValue             => -1
        case NullValue              => Type.Null
      }

      def write(writer: MysqlBufWriter, param: Value) = param match {
        // allows for generic binary values as params to a prepared statement.
        case RawValue(_, _, true, bytes) => writer.writeLengthCodedBytes(bytes)
        // allows for Value types as params to prepared statements
        case ByteValue(b)                => writer.writeByte(b)
        case ShortValue(s)               => writer.writeShortLE(s)
        case IntValue(i)                 => writer.writeIntLE(i)
        case LongValue(l)                => writer.writeLongLE(l)
        case FloatValue(f)               => writer.writeFloatLE(f)
        case DoubleValue(d)              => writer.writeDoubleLE(d)
        case StringValue(s)              => writer.writeLengthCodedString(s, Charset.defaultCharset)
        case _                           => ()
      }
    }
  }

  implicit val timestampCanBeParameter = {
    new CanBeParameter[java.sql.Timestamp] {
      def sizeOf(param: java.sql.Timestamp) = 12
      def typeCode(param: java.sql.Timestamp) = Type.Timestamp
      def write(writer: MysqlBufWriter, param: java.sql.Timestamp) = {
        valueCanBeParameter.write(writer, TimestampValue(param))
      }
    }
  }

  implicit val sqlDateCanBeParameter = {
    new CanBeParameter[java.sql.Date] {
      def sizeOf(param: java.sql.Date) = 5
      def typeCode(param: java.sql.Date) = Type.Date
      def write(writer: MysqlBufWriter, param: java.sql.Date) = {
        valueCanBeParameter.write(writer, DateValue(param))
      }
    }
  }

  implicit val javaDateCanBeParameter = {
    new CanBeParameter[java.util.Date] {
      def sizeOf(param: java.util.Date) = 12
      def typeCode(param: java.util.Date) = Type.DateTime
      def write(writer: MysqlBufWriter, param: java.util.Date) = {
        valueCanBeParameter.write(writer, TimestampValue(new java.sql.Timestamp(param.getTime)))
      }
    }
  }

  implicit val nullCanBeParameter = {
    new CanBeParameter[Null] {
      def sizeOf(param: Null) = 0
      def typeCode(param: Null) = Type.Null
      def write(writer: MysqlBufWriter, param: Null): Unit = ()
    }
  }
}
