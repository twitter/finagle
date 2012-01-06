package com.twitter.finagle.redis

case class ServerError(message: String) extends Exception(message)
case class ClientError(message: String) extends Exception(message)
