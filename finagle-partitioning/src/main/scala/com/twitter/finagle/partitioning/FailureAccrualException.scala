package com.twitter.finagle.partitioning

import com.twitter.finagle._

class FailureAccrualException(message: String, label: String)
    extends RequestException(message, cause = null) {

  def this(message: String) {
    // The super class, SourcedException, assigns the string "unspecified"
    // to the serviceName. Passing it here to preserve functionality.
    this(message, SourcedException.UnspecifiedServiceName)
  }

  serviceName = label
}

/**
 * Thrown when Failure Accrual marks an endpoint dead.
 *
 * @param label the service name
 */
final case class EndpointMarkedDeadException(label: String)
    extends FailureAccrualException("Endpoint is marked dead by failureAccrual", label)
