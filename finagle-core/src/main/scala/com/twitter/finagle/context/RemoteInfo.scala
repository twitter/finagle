package com.twitter.finagle.context

import com.twitter.finagle.tracing.TraceId
import java.net.SocketAddress

/**
 * Contains the remote information for a request, if available.
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

    /**
     * The `SocketAddress` of the upstream client (sender of the request),
     * when available.
     */
    def addr: Option[SocketAddress] = Contexts.local.get(AddressCtx)
  }

  private val NotAvailableStr: String = "Not Available"

  /**
   * Represents the case where remote information is not available,
   * or has not yet been set.
   */
  object NotAvailable extends RemoteInfo {
    override def toString(): String = NotAvailableStr
  }

  /**
   * Represents the case where remote information is available.
   *
   * Includes the upstream's (sender of the request) socket address and id,
   * downstream's (sender of the response) socket address and label, and the
   * trace id.
   */
  case class Available(
    upstreamAddr: Option[SocketAddress],
    upstreamId: Option[String],
    downstreamAddr: Option[SocketAddress],
    downstreamLabel: Option[String],
    traceId: TraceId)
      extends RemoteInfo {
    private[this] def addr(a: Option[SocketAddress]): String = a match {
      case Some(adr) => adr.toString
      case None => NotAvailableStr
    }
    private[this] def id(clientId: Option[String]): String = clientId match {
      case Some(name) => name
      case None => NotAvailableStr
    }

    override def toString(): String =
      s"Upstream Address: ${addr(upstreamAddr)}, Upstream id: ${id(upstreamId)}, " +
        s"Downstream Address: ${addr(downstreamAddr)}, Downstream label: ${id(downstreamLabel)}, " +
        s"Trace Id: $traceId"
  }
}
