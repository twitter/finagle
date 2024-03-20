package com.twitter.finagle.filter

import com.twitter.finagle.Service
import com.twitter.finagle.SimpleFilter
import com.twitter.finagle.server.ServerInfo
import com.twitter.finagle.toggle.Toggle
import com.twitter.util.Future

/**
 * Simple filter which calls the underlying filter if the toggle is enabled or passes the call through
 */
class ToggleAwareSimpleFilter[Req, Rep](underlyingFilter: SimpleFilter[Req, Rep], toggle: Toggle)
    extends SimpleFilter[Req, Rep] {

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    if (ToggleEnabled(toggle)) {
      underlyingFilter(request, service)
    } else {
      service(request)
    }
  }
}

private object ToggleEnabled {
  def apply(toggle: Toggle): Boolean =
    toggle.isEnabled(ServerInfo().id.hashCode)
}
