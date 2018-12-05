package com.twitter.finagle.mysql

import com.twitter.finagle.FailureFlags
import java.sql.SQLNonTransientException

/**
 * Indicates the requested column name was not found.
 */
class ColumnNotFoundException private[mysql] (
  columnName: String,
  val flags: Long = FailureFlags.NonRetryable)
    extends SQLNonTransientException(s"Column not found: '$columnName'")
    with FailureFlags[ColumnNotFoundException] {

  protected def copyWithFlags(flags: Long): ColumnNotFoundException =
    new ColumnNotFoundException(columnName, flags)
}
