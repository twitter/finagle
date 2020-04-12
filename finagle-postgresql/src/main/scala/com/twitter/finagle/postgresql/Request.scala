package com.twitter.finagle.postgresql

trait Request
case object Sync extends Request
case class Query(value: String) extends Request
