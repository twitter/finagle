package com.twitter.finagle.redis

case class ServerError(message: String) extends Exception(message)
case class ClientError(message: String) extends Exception(message)

/**
 * An error caused by failing to cast [[com.twitter.finagle.redis.protocol.Reply]] to a target type
 */
case class ReplyCastFailure(message: String) extends Exception(message)

/**
 * Thrown when the response from a cluster management command could not be decoded correctly.
 */
case class ClusterDecodeError(message: String) extends Exception(message)
