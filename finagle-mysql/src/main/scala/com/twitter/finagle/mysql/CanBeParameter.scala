package com.twitter.finagle.exp.mysql

import com.twitter.finagle.exp.mysql.transport.{Buffer, BufferWriter}

trait CanBeParameter[A] { outer =>
  /**
   * Returns the size of the given parameter in its MySQL binary representation.
   **/
  def sizeOf(param: A): Int

  /**
   * Retrieves the MySQL type code for the given parameter.
   * */
  def typeCode(param: A): Short

  def write(writer: BufferWriter, param: A): BufferWriter
}

object CanBeParameter {
  implicit val stringCanBeParameter: CanBeParameter[String] = {
    new CanBeParameter[String] {
      def sizeOf(param: String): Int = {
        val bytes = param.getBytes(Charset.defaultCharset)
        Buffer.sizeOfLen(bytes.length) + bytes.length
      }

      def typeCode(param: String) = Type.VarChar
      def write(writer: BufferWriter, param: String) = writer.writeLengthCodedString(param)
    }
  }

  implicit val booleanCanBeParameter: CanBeParameter[Boolean] = {
    new CanBeParameter[Boolean] {
      def sizeOf(param: Boolean) = 1
      def typeCode(param: Boolean) = Type.Tiny
      def write(writer: BufferWriter, param: Boolean) = writer.writeBoolean(param)
    }
  }

  implicit val byteCanBeParameter: CanBeParameter[Byte] = {
    new CanBeParameter[Byte] {
      def sizeOf(param: Byte) = 1
      def typeCode(param: Byte) = Type.Tiny
      def write(writer: BufferWriter, param: Byte) = writer.writeByte(param)
    }
  }

  implicit val shortCanBeParameter: CanBeParameter[Short] = {
    new CanBeParameter[Short] {
      def sizeOf(param: Short) = 2
      def typeCode(param: Short) = Type.Short
      def write(writer: BufferWriter, param: Short) = writer.writeShort(param)
    }
  }

  implicit val intCanBeParameter: CanBeParameter[Int] = {
    new CanBeParameter[Int] {
      def sizeOf(param: Int) = 4
      def typeCode(param: Int) = Type.Long
      def write(writer: BufferWriter, param: Int) = writer.writeInt(param)
    }
  }

  implicit val longCanBeParameter: CanBeParameter[Long] = {
    new CanBeParameter[Long] {
      def sizeOf(param: Long) = 8
      def typeCode(param: Long) = Type.LongLong
      def write(writer: BufferWriter, param: Long) = writer.writeLong(param)
    }
  }

  implicit val floatCanBeParameter: CanBeParameter[Float] = {
    new CanBeParameter[Float] {
      def sizeOf(param: Float) = 4
      def typeCode(param: Float) = Type.Float
      def write(writer: BufferWriter, param: Float) = writer.writeFloat(param)
    }
  }

  implicit val doubleCanBeParameter: CanBeParameter[Double] = {
    new CanBeParameter[Double] {
      def sizeOf(param: Double) = 8
      def typeCode(param: Double) = Type.Double
      def write(writer: BufferWriter, param: Double) = writer.writeDouble(param)
    }
  }

  implicit val byteArrayCanBeParameter: CanBeParameter[Array[Byte]] = {
    new CanBeParameter[Array[Byte]] {
      def sizeOf(param: Array[Byte]) = Buffer.sizeOfLen(param.length) + param.length
      def typeCode(param: Array[Byte]) = {
        if (param.length <= 255) Type.TinyBlob
        else if (param.length <= 65535) Type.Blob
        else if (param.length <= 16777215) Type.MediumBlob
        else -1
      }
      def write(writer: BufferWriter, param: Array[Byte]) = writer.writeLengthCodedBytes(param)
    }
  }

  implicit val valueCanBeParameter: CanBeParameter[Value] = {
    new CanBeParameter[Value] {
      def sizeOf(param: Value) = param match {
        case RawValue(_, _, true, b)  => Buffer.sizeOfLen(b.length) + b.length
        case StringValue(s)           => val bytes = s.getBytes(Charset.defaultCharset); Buffer.sizeOfLen(bytes.length) + bytes.length
        case ByteValue(_)             => 1
        case ShortValue(_)            => 2
        case IntValue(_)              => 4
        case LongValue(_)             => 8
        case FloatValue(_)            => 4
        case DoubleValue(_)           => 8
        case EmptyValue               => 0
        case NullValue                => 0
        case RawValue(_, _, false, _) => 0
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

      def write(writer: BufferWriter, param: Value) = param match {
        // allows for generic binary values as params to a prepared statement.
        case RawValue(_, _, true, bytes) => writer.writeLengthCodedBytes(bytes)
        // allows for Value types as params to prepared statements
        case ByteValue(b)                => writer.writeByte(b)
        case ShortValue(s)               => writer.writeShort(s)
        case IntValue(i)                 => writer.writeInt(i)
        case LongValue(l)                => writer.writeLong(l)
        case FloatValue(f)               => writer.writeFloat(f)
        case DoubleValue(d)              => writer.writeDouble(d)
        case StringValue(s)              => writer.writeLengthCodedString(s)
        case EmptyValue                  => writer
        case NullValue                   => writer
        case RawValue(_, _, false, _)    => writer
      }
    }
  }

  implicit val timestampCanBeParameter: CanBeParameter[java.sql.Timestamp] = {
    new CanBeParameter[java.sql.Timestamp] {
      def sizeOf(param: java.sql.Timestamp) = 12
      def typeCode(param: java.sql.Timestamp) = Type.Timestamp
      def write(writer: BufferWriter, param: java.sql.Timestamp) = {
        valueCanBeParameter.write(writer, TimestampValue(param))
      }
    }
  }

  implicit val sqlDateCanBeParameter: CanBeParameter[java.sql.Date] = {
    new CanBeParameter[java.sql.Date] {
      def sizeOf(param: java.sql.Date) = 5
      def typeCode(param: java.sql.Date) = Type.Date
      def write(writer: BufferWriter, param: java.sql.Date) = {
        valueCanBeParameter.write(writer, DateValue(param))
      }
    }
  }

  implicit val javaDateCanBeParameter: CanBeParameter[java.util.Date] = {
    new CanBeParameter[java.util.Date] {
      def sizeOf(param: java.util.Date) = 12
      def typeCode(param: java.util.Date) = Type.DateTime
      def write(writer: BufferWriter, param: java.util.Date) = {
        valueCanBeParameter.write(writer, TimestampValue(new java.sql.Timestamp(param.getTime)))
      }
    }
  }

  implicit val nullCanBeParameter: CanBeParameter[Null] = {
    new CanBeParameter[Null] {
      def sizeOf(param: Null) = 0
      def typeCode(param: Null) = Type.Null
      def write(writer: BufferWriter, param: Null): BufferWriter = writer
    }
  }
}
