package com.twitter.finagle.mysql

case class ClientException(msg: String) extends Exception(msg)
case object IncompatibleServerVersion 
  extends Exception("This client is only compatible with MySQL version 4.1 and later.")
