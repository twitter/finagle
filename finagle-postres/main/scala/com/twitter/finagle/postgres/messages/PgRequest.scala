package com.twitter.finagle.postgres.messages

/*
 * Wrapper around a postgres request.
 */
case class PgRequest(msg: FrontendMessage, flush: Boolean = false)
