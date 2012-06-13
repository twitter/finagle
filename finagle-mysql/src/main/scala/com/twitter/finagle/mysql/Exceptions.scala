package com.twitter.finagle.mysql

case class AuthenticationException(msg: String) extends Exception(msg)
case class DecoderException(msg: String) extends Exception(msg)
case class InvalidResponseException(msg: String) extends Exception(msg)
case object IncompatibleServerVersion 
  extends Exception("This client is only compatible with MySQL version 4.1 and later.")
