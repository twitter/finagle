package com.twitter.finagle.postgresql

import com.twitter.finagle.postgresql.Response.Prepared
import com.twitter.io.Buf

trait Request
object Request {
  case object Sync extends Request
  case class Query(value: String) extends Request

  case class Prepare(statement: String) extends Request
  case class Execute(statement: Prepared, parameters: IndexedSeq[Buf]) extends Request
}
