package com.twitter.finagle.context

import com.twitter.finagle.thrift.ClientId
import com.twitter.finagle.tracing.TraceId
import java.net.SocketAddress

/**
 * Contains the remote information for a request, if available
 */
sealed trait RemoteInfo

object RemoteInfo {

  /**
   * Contains functions to get information about the sender of the request.
   */
  object Upstream {

    /**
     * Context propagated from server to client.
     */
    val AddressCtx = new Contexts.local.Key[SocketAddress]

    def addr: Option[SocketAddress] = Contexts.local.get(AddressCtx)
  }

  /**
   * Represents the case where remote information is not available,
   * or has not yet been set.
   */
  object NotAvailable extends RemoteInfo {
    override def toString(): String = "Not Available"
  }

  /**
   * Represents the case where remote information is available:
   * the upstream (sender of the request) address/client id,
   * downstream (sender of the response) address/client id,
   * and the trace id.
   */
  case class Available(
      upstreamAddr: Option[SocketAddress],
      upstreamId: Option[ClientId],
      downstreamAddr: Option[SocketAddress],
      downstreamId: Option[ClientId],
      traceId: TraceId)
    extends RemoteInfo
  {
    private[this] val upstreamAddrStr = upstreamAddr match {
      case Some(addr) => addr.toString
      case None => "Not Available"
    }
    private[this] val upstreamIdStr = upstreamId match {
      case Some(clientId) => clientId.name
      case None => "Not Available"
    }
    private[this] val downstreamAddrStr = downstreamAddr match {
      case Some(addr) => addr.toString
      case None => "Not Available"
    }
    private[this] val downstreamIdStr = downstreamId match {
      case Some(clientId) => clientId.name
      case None => "Not Available"
    }

    override def toString(): String =
      s"Upstream Address: $upstreamAddrStr, Upstream Client Id: $upstreamIdStr, " +
      s"Downstream Address: $downstreamAddrStr, Downstream Client Id: $downstreamIdStr, " +
      s"Trace Id: $traceId"
  }
}
