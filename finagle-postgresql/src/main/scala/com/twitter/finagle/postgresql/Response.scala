package com.twitter.finagle.postgresql

trait Response
// TODO: remove this
case class BackendResponse(b: BackendMessage) extends Response
