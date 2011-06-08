package com.twitter.finagle.builder

/**
 * Make a com.twitter.ostrich.admin.Service from a finagle ServerBuilder.
 */

import com.twitter.ostrich.admin
import com.twitter.util.Duration
import com.twitter.conversions.time._

import com.twitter.finagle.Service

class ServerBuildertoTwitterService[Req, Rep](
    builder: ServerBuilder[Req, Rep, ServerConfig.Yes, ServerConfig.Yes, ServerConfig.Yes],
    service: Service[Req, Rep],
    gracePeriod: Duration = 10.seconds)
  extends admin.Service
{
  private[this] var server: Option[Server] = None

  def start() {
    if (!server.isDefined)
      server = Some(builder.build(service))
  }

  def shutdown() {
    server foreach { _.close(gracePeriod) }
  }
}
