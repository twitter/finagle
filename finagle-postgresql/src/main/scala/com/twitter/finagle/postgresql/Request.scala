package com.twitter.finagle.postgresql

import com.twitter.finagle.postgresql.Response.Prepared
import com.twitter.finagle.postgresql.Types.Name
import com.twitter.io.Buf

trait Request
object Request {
  case object Sync extends Request
  case class Query(value: String) extends Request

  case class Prepare(statement: String, name: Name = Name.Unnamed) extends Request

  sealed trait Execute extends Request
  case class ExecutePortal(
    prepared: Prepared,
    parameters: IndexedSeq[Buf],
    portalName: Name = Name.Unnamed,
    maxResults: Int = 0,
  ) extends Execute
  case class ResumePortal(name: Name, maxResults: Int) extends Execute
//  case class Execute(prepared: Prepared, parameters: IndexedSeq[Buf], portalName: Name = Name.Unnamed) extends Request
}
