package com.twitter.finagle.mysql

import com.twitter.finagle.FailureFlags
import java.sql.SQLNonTransientException

/**
 * Indicates the requested column was for an unsupported type.
 * For example, asking for a string when the column is a tinyint.
 */
class UnsupportedTypeException private[mysql] (
  columnName: String,
  value: Value,
  val flags: Long = FailureFlags.NonRetryable)
    extends SQLNonTransientException(
      s"For column name '$columnName', value type not supported: ${value.getClass.getName}"
    )
    with FailureFlags[UnsupportedTypeException] {

  protected def copyWithFlags(flags: Long): UnsupportedTypeException =
    new UnsupportedTypeException(columnName, value, flags)
}
