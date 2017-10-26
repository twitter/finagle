package com.twitter.finagle.mysql

/**
 * A `Row` makes it easy to extract [[Value]]'s from a mysql row.
 * Specific [[Value]]'s based on mysql column name can be accessed
 * via the `apply` method.
 */
trait Row {

  /**
   * Contains a Field object for each
   * Column in the Row. The data is 0-indexed
   * so fields(0) contains the column meta-data
   * for the first column in the Row.
   */
  val fields: IndexedSeq[Field]

  /** The values for this Row. */
  val values: IndexedSeq[Value]

  /**
   * Retrieves the index of the column with the given
   * name.
   * @param columnName name of the column.
   * @return Some(Int) if the column
   * exists with the given name. Otherwise, None.
   */
  def indexOf(columnName: String): Option[Int]

  /**
   * Retrieves the Value in the column with the
   * given name.
   * @param columnName name of the column.
   * @return Some(Value) if the column
   * exists with the given name. Otherwise, None.
   */
  def apply(columnName: String): Option[Value] =
    apply(indexOf(columnName))

  protected def apply(columnIndex: Option[Int]): Option[Value] =
    for (idx <- columnIndex) yield values(idx)

  override def toString = (fields zip values).toString
}
