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

/**
 * Thrown when a key has been transferred to a new redis server
 */
case class ClusterMovedError(slotId: Int, host: String) extends Exception()

/**
 * Thrown when a key is being transferred to a new redis server
 */
case class ClusterAskError(slotId: Int, host: String) extends Exception()

/**
 * Thrown when a cluster client has been redirected too many times
 */
case class ClusterTooManyRedirectsError() extends Exception("Reached maximum redirects")

/**
 * Thrown when a cluster client performs a multi-key operation during resharding
 */
case class ClusterTryAgainError() extends Exception()
