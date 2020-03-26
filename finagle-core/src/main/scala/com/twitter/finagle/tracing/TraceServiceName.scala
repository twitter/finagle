package com.twitter.finagle.tracing

/**
 * Exposes an API for setting the global service name for this finagle process
 * which is used to identify traces that belong to the respective process.
 */
object TraceServiceName {
  @volatile private var serviceName: Option[String] = None

  def set(label: Option[String]): Unit = {
    serviceName = label
  }

  def apply(): Option[String] = serviceName
}
