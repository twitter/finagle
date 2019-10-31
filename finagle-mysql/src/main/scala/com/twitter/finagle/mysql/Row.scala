package com.twitter.finagle.mysql

import java.sql.Timestamp
import java.util.TimeZone
import scala.util.control.NonFatal

/**
 * A `Row` allows you to extract [[Value]]'s from a MySQL row.
 *
 * Column values can be accessed by the MySQL column name via the
 * typed `xyzOrNull` and `getXyz` methods. For example, `stringOrNull`
 * and `getString`. The `get`-prefixed methods return `Options` and use `None`
 * to represent a SQL NULL. For SQL NULLs, the `or`-suffixed methods return
 * `null` for Object-types and use a sentinel, like 0, for primitives.
 *
 * Alternatively, [[Value]]'s based on the column name can be accessed
 * via the `apply` method.
 *
 * For example, given the query, `SELECT 'text' AS str_col, 123 AS int_col`,
 * you could extract the columns as such.
 *
 * First, in Scala:
 * {{{
 * import com.twitter.finagle.mysql.Row
 * val row: Row = ???
 *
 * // if the column is not null:
 * val strCol: String = row.stringOrNull("str_col")
 * val intCol: Int = row.intOrZero("int_col")
 *
 * // if the column is nullable:
 * val strCol: Option[String] = row.getString("str_col")
 * val intCol: Option[java.lang.Integer] = row.getInteger("int_col")
 * }}}
 *
 * Then, the same in Java:
 * {{{
 * import com.twitter.finagle.mysql.Row;
 * import scala.Option;
 * Row row = ...
 *
 * // if the column is not null:
 * String strCol = row.stringOrNull("str_col");
 * int intCol = row.intOrZero("int_col");
 *
 * // if the column is nullable:
 * Option<String> strCol = row.getString("str_col");
 * Option<Integer> intCol = row.getInteger("int_col");
 * }}}
 *
 * @see [[PreparedStatement.select]]
 * @see [[Client.select]]
 * @see [[Client.cursor]]
 */
trait Row {

  /**
   * Contains a [[Field]] object for each column in the [[Row]]. The data is 0-indexed
   * so `fields(0)` contains the column metadata for the first column in the [[Row]].
   */
  val fields: IndexedSeq[Field]

  /**
   * The values for this [[Row]].
   * The data is 0-indexed so `values(0)` contains the value for the first
   * column in the [[Row]].
   */
  val values: IndexedSeq[Value]

  /**
   * Retrieves the 0-indexed index of the column with the given name.
   * @param columnName the case sensitive name of the column.
   * @return Some(Int) if the column exists with the given name. Otherwise, None.
   */
  def indexOf(columnName: String): Option[Int]

  /**
   * Retrieves the 0-indexed index of the column with the given name.
   * @param columnName the case sensitive name of the column.
   * @return -1 if the column does not exist, otherwise the index of the column.
   */
  protected def indexOfOrSentinel(columnName: String): Int =
    indexOf(columnName) match {
      case Some(index) => index
      case None => -1
    }

  /**
   * Retrieves the [[Value]] in the column with the given name.
   * @param columnName the case sensitive name of the column.
   * @return Some(Value) if the column exists with the given name. Otherwise, None.
   */
  def apply(columnName: String): Option[Value] =
    apply(indexOf(columnName))

  protected def apply(columnIndex: Option[Int]): Option[Value] =
    columnIndex match {
      case Some(idx) => Some(values(idx))
      case None => None
    }

  override def toString: String = fields.zip(values).toString

  private[this] def unsupportedValue(columnName: String, v: Value): Nothing =
    throw new UnsupportedTypeException(columnName, v)

  private[this] def invalidJsonValue(columnName: String, reason: Throwable): Nothing =
    throw new ValueSerializationException(columnName = columnName, message = reason.getMessage)

  private[this] def columnNotFound(columnName: String): Nothing =
    throw new ColumnNotFoundException(columnName)

  /**
   * Returns a Java primitive `boolean` for the given column name,
   * or `false` if the SQL value is NULL.
   *
   * This is used for MySQL columns that are `tiny` as `boolean`
   * is a synonym for that type.
   *
   * @param columnName the case sensitive name of the column.
   * @see [[getBoolean]]
   * @see [[https://dev.mysql.com/doc/refman/5.5/en/numeric-type-overview.html]]
   * @throws ColumnNotFoundException if the column is not found in the row.
   * @throws UnsupportedTypeException if the MySQL column is not that type.
   */
  def booleanOrFalse(columnName: String): Boolean =
    indexOfOrSentinel(columnName) match {
      case -1 => columnNotFound(columnName)
      case n =>
        values(n) match {
          case ByteValue(v) => v != 0
          case NullValue => false
          case v => unsupportedValue(columnName, v)
        }
    }

  /**
   * Returns `Some` of a boxed Java `Boolean` for the given column name,
   * or `None` if the SQL value is NULL.
   *
   * This is used for MySQL columns that are `tiny` as `boolean`
   * is a synonym for that type.
   *
   * @param columnName the case sensitive name of the column.
   * @see [[booleanOrFalse]]
   * @see [[https://dev.mysql.com/doc/refman/5.5/en/numeric-type-overview.html]]
   * @throws ColumnNotFoundException if the column is not found in the row.
   * @throws UnsupportedTypeException if the MySQL column is not that type.
   */
  def getBoolean(columnName: String): Option[java.lang.Boolean] =
    indexOfOrSentinel(columnName) match {
      case -1 => columnNotFound(columnName)
      case n =>
        values(n) match {
          case ByteValue(v) =>
            if (v != 0) Row.SomeTrue
            else Row.SomeFalse
          case NullValue => None
          case v => unsupportedValue(columnName, v)
        }
    }

  /**
   * Returns a Java primitive `byte` for the given column name,
   * or `0` if the SQL value is NULL.
   *
   * This is used for MySQL columns that are `tiny` and signed.
   *
   * @param columnName the case sensitive name of the column.
   * @see [[getByte]]
   * @throws ColumnNotFoundException if the column is not found in the row.
   * @throws UnsupportedTypeException if the MySQL column is not that type.
   */
  def byteOrZero(columnName: String): Byte =
    indexOfOrSentinel(columnName) match {
      case -1 => columnNotFound(columnName)
      case n =>
        values(n) match {
          case ByteValue(v) => v
          case NullValue => 0
          case v => unsupportedValue(columnName, v)
        }
    }

  /**
   * Returns `Some` of a boxed Java `Byte` for the given column name,
   * or `None` if the SQL value is NULL.
   *
   * This is used for MySQL columns that are signed and `tiny`.
   *
   * @param columnName the case sensitive name of the column.
   * @see [[byteOrZero]]
   * @throws ColumnNotFoundException if the column is not found in the row.
   * @throws UnsupportedTypeException if the MySQL column is not that type.
   */
  def getByte(columnName: String): Option[java.lang.Byte] =
    indexOfOrSentinel(columnName) match {
      case -1 => columnNotFound(columnName)
      case n =>
        values(n) match {
          case ByteValue(v) => Some(Byte.box(v))
          case NullValue => None
          case v => unsupportedValue(columnName, v)
        }
    }

  /**
   * Returns a Java primitive `short` for the given column name,
   * or `0` if the SQL value is NULL.
   *
   * This can be used for MySQL columns that are `tiny`, or
   * signed and `short`.
   *
   * @param columnName the case sensitive name of the column.
   * @see [[getShort]]
   * @throws ColumnNotFoundException if the column is not found in the row.
   * @throws UnsupportedTypeException if the MySQL column is not a supported type.
   */
  def shortOrZero(columnName: String): Short =
    indexOfOrSentinel(columnName) match {
      case -1 => columnNotFound(columnName)
      case n =>
        values(n) match {
          case ShortValue(v) => v
          case ByteValue(v) => v.toShort
          case NullValue => 0
          case v => unsupportedValue(columnName, v)
        }
    }

  /**
   * Returns `Some` of a boxed Java `Short` for the given column name,
   * or `None` if the SQL value is NULL.
   *
   * This can be used for MySQL columns that are `tiny`, or
   * signed and `short`.
   *
   * @param columnName the case sensitive name of the column.
   * @see [[shortOrZero]]
   * @throws ColumnNotFoundException if the column is not found in the row.
   * @throws UnsupportedTypeException if the MySQL column is not a supported type.
   */
  def getShort(columnName: String): Option[java.lang.Short] =
    indexOfOrSentinel(columnName) match {
      case -1 => columnNotFound(columnName)
      case n =>
        values(n) match {
          case ShortValue(v) => Some(Short.box(v))
          case ByteValue(v) => Some(Short.box(v.toShort))
          case NullValue => None
          case v => unsupportedValue(columnName, v)
        }
    }

  /**
   * Returns a Java primitive `int` for the given column name,
   * or `0` if the SQL value is NULL.
   *
   * This can be used for MySQL columns that are `tiny`, `short`,
   * `int24`, or signed and `long`.
   *
   * @param columnName the case sensitive name of the column.
   * @see [[getInteger]]
   * @throws ColumnNotFoundException if the column is not found in the row.
   * @throws UnsupportedTypeException if the MySQL column is not a supported type.
   */
  def intOrZero(columnName: String): Int =
    indexOfOrSentinel(columnName) match {
      case -1 => columnNotFound(columnName)
      case n =>
        values(n) match {
          case IntValue(v) => v
          case ShortValue(v) => v.toInt
          case ByteValue(v) => v.toInt
          case NullValue => 0
          case v => unsupportedValue(columnName, v)
        }
    }

  /**
   * Returns `Some` of a boxed Java `Integer` for the given column name,
   * or `None` if the SQL value is NULL.
   *
   * This can be used for MySQL columns that are `tiny`, `short`,
   * `int24`, or signed and `long`.
   *
   * @param columnName the case sensitive name of the column.
   * @see [[intOrZero]]
   * @throws ColumnNotFoundException if the column is not found in the row.
   * @throws UnsupportedTypeException if the MySQL column is not a supported type.
   */
  def getInteger(columnName: String): Option[java.lang.Integer] =
    indexOfOrSentinel(columnName) match {
      case -1 => columnNotFound(columnName)
      case n =>
        values(n) match {
          case IntValue(v) => Some(Int.box(v))
          case ShortValue(v) => Some(Int.box(v.toInt))
          case ByteValue(v) => Some(Int.box(v.toInt))
          case NullValue => None
          case v => unsupportedValue(columnName, v)
        }
    }

  /**
   * Returns a Java primitive `long` for the given column name,
   * or `0` if the SQL value is NULL.
   *
   * This can be used for MySQL columns that are `tiny`, `short`,
   * `int24`, `long`, or signed and `longlong`
   *
   * @param columnName the case sensitive name of the column.
   * @see [[getLong]]
   * @throws ColumnNotFoundException if the column is not found in the row.
   * @throws UnsupportedTypeException if the MySQL column is not a supported type.
   */
  def longOrZero(columnName: String): Long =
    indexOfOrSentinel(columnName) match {
      case -1 => columnNotFound(columnName)
      case n =>
        values(n) match {
          case LongValue(v) => v
          case IntValue(v) => v.toLong
          case ShortValue(v) => v.toLong
          case ByteValue(v) => v.toLong
          case NullValue => 0L
          case v => unsupportedValue(columnName, v)
        }
    }

  /**
   * Returns `Some` of a boxed Java `Long` for the given column name,
   * or `None` if the SQL value is NULL.
   *
   * This can be used for MySQL columns that are `tiny`, `short`,
   * `int24`, `long`, or signed and `longlong`
   *
   * @param columnName the case sensitive name of the column.
   * @see [[longOrZero]]
   * @throws ColumnNotFoundException if the column is not found in the row.
   * @throws UnsupportedTypeException if the MySQL column is not a supported type.
   */
  def getLong(columnName: String): Option[java.lang.Long] =
    indexOfOrSentinel(columnName) match {
      case -1 => columnNotFound(columnName)
      case n =>
        values(n) match {
          case LongValue(v) => Some(Long.box(v))
          case IntValue(v) => Some(Long.box(v.toLong))
          case ShortValue(v) => Some(Long.box(v.toLong))
          case ByteValue(v) => Some(Long.box(v.toLong))
          case NullValue => None
          case v => unsupportedValue(columnName, v)
        }
    }

  /**
   * Returns a Java primitive `float` for the given column name,
   * or `0` if the SQL value is NULL.
   *
   * This is used for MySQL columns that are `float`.
   *
   * @param columnName the case sensitive name of the column.
   * @see [[getFloat]]
   * @throws ColumnNotFoundException if the column is not found in the row.
   * @throws UnsupportedTypeException if the MySQL column is not a supported type.
   */
  def floatOrZero(columnName: String): Float =
    indexOfOrSentinel(columnName) match {
      case -1 => columnNotFound(columnName)
      case n =>
        values(n) match {
          case FloatValue(v) => v
          case NullValue => 0.0f
          case v => unsupportedValue(columnName, v)
        }
    }

  /**
   * Returns `Some` of a boxed Java `Float` for the given column name,
   * or `None` if the SQL value is NULL.
   *
   * This is used for MySQL columns that are `float`.
   *
   * @param columnName the case sensitive name of the column.
   * @see [[floatOrZero]]
   * @throws ColumnNotFoundException if the column is not found in the row.
   * @throws UnsupportedTypeException if the MySQL column is not a supported type.
   */
  def getFloat(columnName: String): Option[java.lang.Float] =
    indexOfOrSentinel(columnName) match {
      case -1 => columnNotFound(columnName)
      case n =>
        values(n) match {
          case FloatValue(v) => Some(Float.box(v))
          case NullValue => None
          case v => unsupportedValue(columnName, v)
        }
    }

  /**
   * Returns a Java primitive `double` for the given column name,
   * or `0` if the SQL value is NULL.
   *
   * This can be used for MySQL columns that are `float` or
   * `double`.
   *
   * @param columnName the case sensitive name of the column.
   * @see [[getDouble]]
   * @throws ColumnNotFoundException if the column is not found in the row.
   * @throws UnsupportedTypeException if the MySQL column is not a supported type.
   */
  def doubleOrZero(columnName: String): Double =
    indexOfOrSentinel(columnName) match {
      case -1 => columnNotFound(columnName)
      case n =>
        values(n) match {
          case DoubleValue(v) => v
          case FloatValue(v) => v.toDouble
          case NullValue => 0.0
          case v => unsupportedValue(columnName, v)
        }
    }

  /**
   * Returns `Some` of a boxed Java `Double` for the given column name,
   * or `None` if the SQL value is NULL.
   *
   * This can be used for MySQL columns that are `float` or
   * `double`.
   *
   * @param columnName the case sensitive name of the column.
   * @see [[doubleOrZero(]]
   * @throws ColumnNotFoundException if the column is not found in the row.
   * @throws UnsupportedTypeException if the MySQL column is not a supported type.
   */
  def getDouble(columnName: String): Option[java.lang.Double] =
    indexOfOrSentinel(columnName) match {
      case -1 => columnNotFound(columnName)
      case n =>
        values(n) match {
          case DoubleValue(v) => Some(Double.box(v))
          case FloatValue(v) => Some(Double.box(v.toDouble))
          case NullValue => None
          case v => unsupportedValue(columnName, v)
        }
    }

  /**
   * Returns a Java `String` for the given column name,
   * or `null` if the SQL value is NULL.
   *
   * This can be used for MySQL columns that are `varchar`, `string`, or `varstring`,
   * Also supports `tinyblob`, `blob`, and `mediumblob` if the field's charset
   * is not the binary charset.
   *
   * @param columnName the case sensitive name of the column.
   * @see [[getString]]
   * @throws ColumnNotFoundException if the column is not found in the row.
   * @throws UnsupportedTypeException if the MySQL column is not a supported type.
   */
  def stringOrNull(columnName: String): String = {
    indexOfOrSentinel(columnName) match {
      case -1 => columnNotFound(columnName)
      case n =>
        values(n) match {
          case StringValue(v) => v
          case EmptyValue => Row.EmptyString
          case NullValue => null
          case v => unsupportedValue(columnName, v)
        }
    }
  }

  /**
   * Returns `Some` of a `String` for the given column name,
   * or `None` if the SQL value is NULL.
   *
   * This can be used for MySQL columns that are `varchar`, `string`, or `varstring`,
   * Also supports `tinyblob`, `blob`, and `mediumblob` if the field's charset
   * is not the binary charset.
   *
   * @param columnName the case sensitive name of the column.
   * @see [[stringOrNull]]
   * @throws ColumnNotFoundException if the column is not found in the row.
   * @throws UnsupportedTypeException if the MySQL column is not a supported type.
   */
  def getString(columnName: String): Option[String] =
    Option(stringOrNull(columnName))

  /**
   * Returns a `BigInt` for the given column name,
   * or `null` if the SQL value is NULL.
   *
   * This can be used for MySQL columns that are `tiny`, `short`,
   * `int24`, `long`, or `longlong`.
   *
   * @param columnName the case sensitive name of the column.
   * @see [[getBigInt]]
   * @throws ColumnNotFoundException if the column is not found in the row.
   * @throws UnsupportedTypeException if the MySQL column is not a supported type.
   */
  def bigIntOrNull(columnName: String): BigInt =
    indexOfOrSentinel(columnName) match {
      case -1 => columnNotFound(columnName)
      case n =>
        values(n) match {
          case BigIntValue(v) => v
          case ByteValue(v) => BigInt(v)
          case ShortValue(v) => BigInt(v)
          case IntValue(v) => BigInt(v)
          case LongValue(v) => BigInt(v)
          case NullValue => null
          case v => unsupportedValue(columnName, v)
        }
    }

  /**
   * Returns `Some` of a `BigInt` for the given column name,
   * or `None` if the SQL value is NULL.
   *
   * This can be used for MySQL columns that are `tiny`, `short`,
   * `int24`, `long`, or `longlong`.
   *
   * @param columnName the case sensitive name of the column.
   * @see [[bigIntOrNull]]
   * @throws ColumnNotFoundException if the column is not found in the row.
   * @throws UnsupportedTypeException if the MySQL column is not a supported type.
   */
  def getBigInt(columnName: String): Option[BigInt] =
    Option(bigIntOrNull(columnName))

  /**
   * Returns a `BigDecimal` for the given column name,
   * or `null` if the SQL value is NULL.
   *
   * This can be used for MySQL columns that are `NewDecimal`,
   * `float`, or `double`.
   *
   * @param columnName the case sensitive name of the column.
   * @see [[getBigDecimal]]
   * @throws ColumnNotFoundException if the column is not found in the row.
   * @throws UnsupportedTypeException if the MySQL column is not a supported type.
   */
  def bigDecimalOrNull(columnName: String): BigDecimal =
    indexOfOrSentinel(columnName) match {
      case -1 => columnNotFound(columnName)
      case n =>
        values(n) match {
          case BigDecimalValue(v) => v
          case FloatValue(v) => BigDecimal.decimal(v)
          case DoubleValue(v) => BigDecimal.decimal(v)
          case NullValue => null
          case v => unsupportedValue(columnName, v)
        }
    }

  /**
   * Returns `Some` of a `BigDecimal` for the given column name,
   * or `None` if the SQL value is NULL.
   *
   * This can be used for MySQL columns that are `NewDecimal`,
   * `float`, or `double`.
   *
   * @param columnName the case sensitive name of the column.
   * @see [[bigDecimalOrNull]]
   * @throws ColumnNotFoundException if the column is not found in the row.
   * @throws UnsupportedTypeException if the MySQL column is not a supported type.
   */
  def getBigDecimal(columnName: String): Option[BigDecimal] =
    Option(bigDecimalOrNull(columnName))

  /**
   * Returns a `Array[Byte]` for the given column name,
   * or `null` if the SQL value is NULL.
   *
   * This can be used for MySQL columns that are `tinyblob`,
   * `mediumblob`, `blob`, `binary`, or `varbinary`.
   *
   * @param columnName the case sensitive name of the column.
   * @see [[getBytes]]
   * @throws ColumnNotFoundException if the column is not found in the row.
   * @throws UnsupportedTypeException if the MySQL column is not that type.
   */
  def bytesOrNull(columnName: String): Array[Byte] =
    indexOfOrSentinel(columnName) match {
      case -1 => columnNotFound(columnName)
      case n =>
        values(n) match {
          case RawValue(typ, _, _, bytes) if Row.isBinary(typ) => bytes
          case EmptyValue => Array.emptyByteArray
          case NullValue => null
          case v => unsupportedValue(columnName, v)
        }
    }

  /**
   * Returns `Some` of an `Array[Byte]` for the given column name,
   * or `None` if the SQL value is NULL.
   *
   * This can be used for MySQL columns that are `tinyblob`,
   * `mediumblob`, `blob`, `binary`, or `varbinary`.
   *
   * @param columnName the case sensitive name of the column.
   * @see [[bytesOrNull]]
   * @throws ColumnNotFoundException if the column is not found in the row.
   * @throws UnsupportedTypeException if the MySQL column is not that type.
   */
  def getBytes(columnName: String): Option[Array[Byte]] =
    Option(bytesOrNull(columnName))

  /**
   * Returns a `java.sql.Timestamp` for the given column name,
   * or `null` if the SQL value is NULL.
   *
   * This can be used for MySQL columns that are `timestamp` or `datetime`.
   *
   * @param columnName the case sensitive name of the column.
   * @param timeZone the `TimeZone` used to parse the data.
   * @see [[getTimestamp]]
   * @throws ColumnNotFoundException if the column is not found in the row.
   * @throws UnsupportedTypeException if the MySQL column is not that type.
   */
  def timestampOrNull(columnName: String, timeZone: TimeZone): Timestamp =
    indexOfOrSentinel(columnName) match {
      case -1 => columnNotFound(columnName)
      case n =>
        values(n) match {
          case value if TimestampValue.isTimestamp(value) =>
            TimestampValue.fromValue(value, timeZone) match {
              case Some(timestamp) => timestamp
              case None => unsupportedValue(columnName, value)
            }
          case NullValue => null
          case v => unsupportedValue(columnName, v)
        }
    }

  /**
   * Returns `Some` of an `java.sql.Timestamp` for the given column name,
   * or `None` if the SQL value is NULL.
   *
   * This can be used for MySQL columns that are `timestamp` or `datetime`.
   *
   * @param columnName the case sensitive name of the column.
   * @param timeZone the `TimeZone` used to parse the data.
   * @see [[timestampOrNull]]
   * @throws ColumnNotFoundException if the column is not found in the row.
   * @throws UnsupportedTypeException if the MySQL column is not that type.
   */
  def getTimestamp(columnName: String, timeZone: TimeZone): Option[Timestamp] =
    Option(timestampOrNull(columnName, timeZone))

  /**
   * Returns a `java.sql.Date` for the given column name,
   * or `null` if the SQL value is NULL.
   *
   * This can be used for MySQL columns that are `date`.
   *
   * @param columnName the case sensitive name of the column.
   * @see [[getJavaSqlDate]]
   * @throws ColumnNotFoundException if the column is not found in the row.
   * @throws UnsupportedTypeException if the MySQL column is not a supported type.
   */
  def javaSqlDateOrNull(columnName: String): java.sql.Date =
    indexOfOrSentinel(columnName) match {
      case -1 => columnNotFound(columnName)
      case n =>
        values(n) match {
          case DateValue(v) => v
          case NullValue => null
          case v => unsupportedValue(columnName, v)
        }
    }

  /**
   * Returns `Some` of a `java.sql.Date` for the given column name,
   * or `None` if the SQL value is NULL.
   *
   * This can be used for MySQL columns that are `date`.
   *
   * @param columnName the case sensitive name of the column.
   * @see [[javaSqlDateOrNull(]]
   * @throws ColumnNotFoundException if the column is not found in the row.
   * @throws UnsupportedTypeException if the MySQL column is not a supported type.
   */
  def getJavaSqlDate(columnName: String): Option[java.sql.Date] =
    Option(javaSqlDateOrNull(columnName))

  /**
   * Read the value of MySQL `json` column as type `T` or `null` if the SQL value is NULL.
   *
   * @param columnName the case sensitive name of the column.
   * @param objMapper the `objMapper` used to parse the json column value into type `T`.
   *
   * @see [[getJsonAsObject]]
   * @throws ColumnNotFoundException if the column is not found in the row.
   * @throws UnsupportedTypeException if the MySQL column is not MySQL `json` type.
   * @throws ValueSerializationException if the MySQL json column value cannot be serialized as `T`.
   */
  def jsonAsObjectOrNull[T >: Null](
    columnName: String,
    objMapper: JsonValue.Serializer
  )(
    implicit m: Manifest[T]
  ): T = getJsonAsObject[T](columnName, objMapper).orNull

  /**
   * Read the value of MySQL `json` column as `Some(T)` or `None` if the SQL value is NULL.
   *
   * @param columnName the case sensitive name of the column.
   * @param objMapper the `objMapper` used to parse the json column value into type `T`.
   *
   * @see [[jsonAsObjectOrNull]]
   * @throws ColumnNotFoundException if the column is not found in the row.
   * @throws UnsupportedTypeException if the MySQL column is not MySQL `json` type.
   * @throws ValueSerializationException if the MySQL json column value cannot be serialized as `T`.
   */
  def getJsonAsObject[T](
    columnName: String,
    objMapper: JsonValue.Serializer
  )(
    implicit m: Manifest[T]
  ): Option[T] = {
    Option(jsonBytesOrNull(columnName)).map { bytes =>
      try {
        objMapper.readValue[T](bytes)
      } catch {
        case NonFatal(err) => invalidJsonValue(columnName, err)
      }
    }
  }

  /**
   * Read the `Array[Byte]` value of MySQL `json` column or `null` if the SQL value is NULL.
   *
   * @param columnName the case sensitive name of the column.
   *
   * @see [[jsonAsObjectOrNull]]
   * @throws ColumnNotFoundException if the column is not found in the row.
   * @throws UnsupportedTypeException if the MySQL column is not MySQL `json` type.
   */
  def jsonBytesOrNull(columnName: String): Array[Byte] = {
    indexOfOrSentinel(columnName) match {
      case -1 => columnNotFound(columnName)
      case n =>
        values(n) match {
          case NullValue => null
          case value =>
            JsonValue.fromValue(value) match {
              case Some(bytes) => bytes
              case None => unsupportedValue(columnName, value)
            }
        }
    }
  }
}

private object Row {
  private val EmptyString: String = ""
  private val SomeTrue: Option[java.lang.Boolean] = Some(java.lang.Boolean.TRUE)
  private val SomeFalse: Option[java.lang.Boolean] = Some(java.lang.Boolean.FALSE)

  private def isBinary(typ: Short): Boolean =
    typ match {
      case Type.TinyBlob => true
      case Type.MediumBlob => true
      case Type.Blob => true
      // used for `binary` columns
      case Type.String => true
      // used for `varbinary` columns
      case Type.VarString => true
      case _ => false
    }

}

/** Exposed for testing in Java */
private abstract class AbstractRow extends Row
