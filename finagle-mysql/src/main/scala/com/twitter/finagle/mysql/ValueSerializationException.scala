package com.twitter.finagle.mysql

import com.twitter.finagle.FailureFlags
import java.sql.SQLNonTransientException

/**
 * Thrown if unable to serialize a value of the column.
 */
class ValueSerializationException private[mysql] (
  columnName: String,
  message: String,
  val flags: Long = FailureFlags.NonRetryable)
    extends SQLNonTransientException(
      s"Unable to serialize a value from column '$columnName'. $message")
    with FailureFlags[ValueSerializationException] {

  protected def copyWithFlags(flags: Long): ValueSerializationException =
    new ValueSerializationException(columnName, message, flags)
}
