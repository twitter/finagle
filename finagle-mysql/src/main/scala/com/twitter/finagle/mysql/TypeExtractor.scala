package com.twitter.finagle.exp.mysql

import annotation.implicitNotFound
import java.sql.Timestamp

@implicitNotFound(msg = "Don't know how to extract ${T} from a Value.")
trait TypeExtractor[T] {
  def extractFrom(v: Value): Option[T]
}

object TypeExtractor {
  implicit object StringExtractor extends TypeExtractor[String] {
    def extractFrom(v: Value): Option[String] = v match {
      case StringValue(s) => Some(s)
      case _              => None
    }
  }
  implicit object BooleanExtractor extends TypeExtractor[Boolean] {
    def extractFrom(v: Value): Option[Boolean] = v match {
      case ByteValue(b) => Some(b == 1)
      case _            => None
    }
  }
  implicit object ByteExtractor extends TypeExtractor[Byte] {
    def extractFrom(v: Value): Option[Byte] = v match {
      case ByteValue(b) => Some(b) 
      case _            => None
    }
  }
  implicit object ShortExtractor extends TypeExtractor[Short] {
    def extractFrom(v: Value): Option[Short] = v match {
      case ShortValue(s) => Some(s)
      case _             => None
    }
  }
  implicit object IntExtractor extends TypeExtractor[Int] {
    def extractFrom(v: Value): Option[Int] = v match {
      case IntValue(i) => Some(i)
      case _           => None
    }
  }
  implicit object LongExtractor extends TypeExtractor[Long] {
    def extractFrom(v: Value): Option[Long] = v match {
      case LongValue(l) => Some(l)
      case _ => None
    }
  }
  implicit object FloatExtractor extends TypeExtractor[Float] {
    def extractFrom(v: Value): Option[Float] = v match {
      case FloatValue(f) => Some(f)
      case _             => None
    }
  }
  implicit object DoubleExtractor extends TypeExtractor[Double] {
    def extractFrom(v: Value): Option[Double] = v match {
      case DoubleValue(d) => Some(d)
      case _              => None
    }
  }
  implicit object TimestampExtractor extends TypeExtractor[Timestamp] {
    private val UTC = java.util.TimeZone.getTimeZone("UTC")
    private val timestampValue = new TimestampValue(UTC, UTC)
    def extractFrom(v: Value): Option[Timestamp] = timestampValue.unapply(v)
  }
  implicit def nullableExtractor[T : TypeExtractor]: TypeExtractor[Option[T]] = new TypeExtractor[Option[T]] {
    override def extractFrom(v: Value) = Option(v match {
      case NullValue | EmptyValue => None
      case v => implicitly[TypeExtractor[T]].extractFrom(v)
    })
  }
}
