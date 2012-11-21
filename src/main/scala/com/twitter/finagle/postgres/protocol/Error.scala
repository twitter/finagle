package com.twitter.finagle.postgres.protocol

class ServerError(message : String) extends Exception(message)
class ClientError(message : String) extends Exception(message)

object Errors {
  def client(message : String) = new ClientError(message)
  def server(message : String) = new ServerError(message)
}