package com.twitter.finagle.redis

case class ServerError(message: String) extends Exception(message)
case class ClientError(message: String) extends Exception(message)

/**
 * An error caused by failing to cast [[com.twitter.finagle.redis.protocol.Reply]] to a target type
 */
case class ReplyCastFailure(message: String) extends Exception(message)