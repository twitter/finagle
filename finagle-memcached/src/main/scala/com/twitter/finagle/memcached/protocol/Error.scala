package com.twitter.finagle.memcached.protocol

/**
 * Indicates that a request failed because an attempt was made to decode a
 * non-existent memcached command.
 */
class NonexistentCommand(message: String) extends Exception(message)

/**
 * A catch-all exception class for memcached client-related failures.
 */
class ClientError(message: String) extends Exception(message)

/**
 * A catch-all exception class for memcached server-related failures.
 */
class ServerError(message: String) extends Exception(message)
