package com.twitter.finagle.memcached.protocol

class NonexistentCommand(message: String) extends Exception(message)
class ClientError(message: String) extends Exception(message)
class ServerError(message: String) extends Exception(message)
