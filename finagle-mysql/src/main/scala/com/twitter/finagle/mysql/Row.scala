package com.twitter.finagle.mysql

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

  private[this] def columnNotFound(columnName: String): Nothing =
    throw new ColumnNotFoundException(columnName)

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
    apply(columnName) match {
      case Some(ByteValue(v)) => v
      case Some(NullValue) => 0
      case Some(v) => unsupportedValue(columnName, v)
      case None => columnNotFound(columnName)
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
    apply(columnName) match {
      case Some(ByteValue(v)) => Some(Byte.box(v))
      case Some(NullValue) => None
      case Some(v) => unsupportedValue(columnName, v)
      case None => columnNotFound(columnName)
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
    apply(columnName) match {
      case Some(ShortValue(v)) => v
      case Some(ByteValue(v)) => v.toShort
      case Some(NullValue) => 0
      case Some(v) => unsupportedValue(columnName, v)
      case None => columnNotFound(columnName)
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
    apply(columnName) match {
      case Some(ShortValue(v)) => Some(Short.box(v))
      case Some(ByteValue(v)) => Some(Short.box(v.toShort))
      case Some(NullValue) => None
      case Some(v) => unsupportedValue(columnName, v)
      case None => columnNotFound(columnName)
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
    apply(columnName) match {
      case Some(IntValue(v)) => v
      case Some(ShortValue(v)) => v.toInt
      case Some(ByteValue(v)) => v.toInt
      case Some(NullValue) => 0
      case Some(v) => unsupportedValue(columnName, v)
      case None => columnNotFound(columnName)
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
    apply(columnName) match {
      case Some(IntValue(v)) => Some(Int.box(v))
      case Some(ShortValue(v)) => Some(Int.box(v.toInt))
      case Some(ByteValue(v)) => Some(Int.box(v.toInt))
      case Some(NullValue) => None
      case Some(v) => unsupportedValue(columnName, v)
      case None => columnNotFound(columnName)
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
    apply(columnName) match {
      case Some(LongValue(v)) => v
      case Some(IntValue(v)) => v.toLong
      case Some(ShortValue(v)) => v.toLong
      case Some(ByteValue(v)) => v.toLong
      case Some(NullValue) => 0L
      case Some(v) => unsupportedValue(columnName, v)
      case None => columnNotFound(columnName)
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
    apply(columnName) match {
      case Some(LongValue(v)) => Some(Long.box(v))
      case Some(IntValue(v)) => Some(Long.box(v.toLong))
      case Some(ShortValue(v)) => Some(Long.box(v.toLong))
      case Some(ByteValue(v)) => Some(Long.box(v.toLong))
      case Some(NullValue) => None
      case Some(v) => unsupportedValue(columnName, v)
      case None => columnNotFound(columnName)
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
    apply(columnName) match {
      case Some(FloatValue(v)) => v
      case Some(NullValue) => 0.0f
      case Some(v) => unsupportedValue(columnName, v)
      case None => columnNotFound(columnName)
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
    apply(columnName) match {
      case Some(FloatValue(v)) => Some(Float.box(v))
      case Some(NullValue) => None
      case Some(v) => unsupportedValue(columnName, v)
      case None => columnNotFound(columnName)
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
    apply(columnName) match {
      case Some(DoubleValue(v)) => v
      case Some(FloatValue(v)) => v.toDouble
      case Some(NullValue) => 0.0
      case Some(v) => unsupportedValue(columnName, v)
      case None => columnNotFound(columnName)
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
    apply(columnName) match {
      case Some(DoubleValue(v)) => Some(Double.box(v))
      case Some(FloatValue(v)) => Some(Double.box(v.toDouble))
      case Some(NullValue) => None
      case Some(v) => unsupportedValue(columnName, v)
      case None => columnNotFound(columnName)
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
  def stringOrNull(columnName: String): String =
    apply(columnName) match {
      case Some(StringValue(v)) => v
      case Some(EmptyValue) => Row.EmptyString
      case Some(NullValue) => null
      case Some(v) => unsupportedValue(columnName, v)
      case None => columnNotFound(columnName)
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
    apply(columnName) match {
      case Some(BigIntValue(v)) => v
      case Some(ByteValue(v)) => BigInt(v)
      case Some(ShortValue(v)) => BigInt(v)
      case Some(IntValue(v)) => BigInt(v)
      case Some(LongValue(v)) => BigInt(v)
      case Some(NullValue) => null
      case Some(v) => unsupportedValue(columnName, v)
      case None => columnNotFound(columnName)
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
    apply(columnName) match {
      case Some(BigDecimalValue(v)) => v
      case Some(FloatValue(v)) => BigDecimal.decimal(v)
      case Some(DoubleValue(v)) => BigDecimal.decimal(v)
      case Some(NullValue) => null
      case Some(v) => unsupportedValue(columnName, v)
      case None => columnNotFound(columnName)
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
    apply(columnName) match {
      case Some(DateValue(v)) => v
      case Some(NullValue) => null
      case Some(v) => unsupportedValue(columnName, v)
      case None => columnNotFound(columnName)
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

}

private object Row {
  private val EmptyString: String = ""

}

/** Exposed for testing in Java */
private abstract class AbstractRow extends Row
