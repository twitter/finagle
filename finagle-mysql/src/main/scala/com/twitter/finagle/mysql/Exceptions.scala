package com.twitter.finagle.exp.mysql

case class ClientError(msg: String) extends Exception(msg)
case class ServerError(msg: String) extends Exception(msg)
case class IncompatibleServer(msg: String) extends Exception(msg)