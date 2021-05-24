package com.twitter.finagle.client

import com.twitter.finagle.Service
import com.twitter.finagle.service.Filterable

/**
 * ServicePerEndpointBuilder creates `ServicePerEndpoint` typed interfaces from underlying
 * `Service[Req, Rep]` typed services.
 */
trait ServicePerEndpointBuilder[Req, Rep, ServicePerEndpoint <: Filterable[ServicePerEndpoint]] {

  /**
   * A runtime class for this service.
   */
  def serviceClass: Class[_]

  /**
   * Build a client ServicePerEndpoint wrapping an underlying service.
   *
   * @param service An underlying client service.
   * @note service is passed as a factory function, to allow lazy or repeatable construction.
   */
  def servicePerEndpoint(service: => Service[Req, Rep]): ServicePerEndpoint
}
