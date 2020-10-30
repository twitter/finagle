package com.twitter.finagle.postgresql

import com.twitter.finagle.postgresql.Response.Prepared
import com.twitter.finagle.postgresql.Types.Name
import com.twitter.finagle.postgresql.Types.WireValue

trait Request
object Request {

  /**
   * Synthetic request to extract the current connection's parameters.
   *
   * During connection establishment (i.e.: before any request is sent) the backend sends a set
   * of parameter status values to the client. These are accumulated in the dispatcher which isn't
   * accessible by the client.
   *
   * This request can be used to obtain those parameters.
   * Note that in order for these to be meaningful for subsequent requests, those
   * must be made on the same connection. For example:
   *
   * {{{
   *   val sf: ServiceFactory[Request, Response]
   *   sf().flatMap { service =>
   *     service(Request.ConnectionParameters)
   *       .flatMap { params =>
   *         val something = withParams(params)
   *         // subsequent request on the same connection.
   *         service(Request.Query(...))
   *       }
   *   }
   * }}}
   *
   *
   * @see [[Response.ConnectionParameters]]
   */
  case object ConnectionParameters extends Request
  case object Sync extends Request
  case class Query(value: String) extends Request

  case class Prepare(statement: String, name: Name = Name.Unnamed) extends Request

  sealed trait Execute extends Request
  case class ExecutePortal(
    prepared: Prepared,
    parameters: Seq[WireValue],
    portalName: Name = Name.Unnamed,
    maxResults: Int = 0,
  ) extends Execute
  case class ResumePortal(name: Name, maxResults: Int) extends Execute
}
