package com.twitter.finagle.mysql

import com.twitter.finagle.FailureFlags
import java.sql.SQLNonTransientException

/**
 * Indicates that the server lacks required capabilities
 */
class InsufficientServerCapabilitiesException(
  required: Capability,
  available: Capability,
  val flags: Long = FailureFlags.NonRetryable)
    extends SQLNonTransientException(
      s"Insufficient capabilities: available: '$available', required: '$required'")
    with FailureFlags[InsufficientServerCapabilitiesException] {

  protected def copyWithFlags(flags: Long): InsufficientServerCapabilitiesException =
    new InsufficientServerCapabilitiesException(required, available, flags)
}
