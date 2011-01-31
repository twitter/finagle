package com.twitter.finagle.memcached.protocol

sealed abstract class Error(message: String) extends Exception(message)

class NonexistentCommand(message: String) extends Error(message)
class ClientError(message: String) extends Error(message)
class ServerError(message: String) extends Error(message)