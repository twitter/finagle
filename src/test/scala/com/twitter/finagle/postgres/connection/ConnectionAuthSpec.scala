package com.twitter.finagle.postgres.connection

import com.twitter.finagle.postgres.Spec
import com.twitter.finagle.postgres.messages._

class ConnectionAuthSpec extends Spec {
  val salt = Array[Byte](1, 2, 3)

  "A postgres connection" should {
    "connect to a client" in {
      val connection = new Connection()

      connection.send(StartupMessage("test", "test"))

      val response = connection.receive(AuthenticationOk())
      response must equal(None)

      val response2 = connection.receive(ReadyForQuery('I'))
      response2 must equal(Some(AuthenticatedResponse(Map(), -1, -1)))
    }

    "return an error during failed connection" in {
      val connection = new Connection()

      connection.send(StartupMessage("test", "test"))
      val response = connection.receive(ErrorResponse())
      response must equal(Some(Error(None)))
    }

    "request a clear text password" in {
      val connection = new Connection()

      connection.send(StartupMessage("test", "test"))
      val response = connection.receive(AuthenticationCleartextPassword())
      response must equal(Some(PasswordRequired(ClearText)))
    }

    "request an md5 password" in {
      val connection = new Connection()

      connection.send(StartupMessage("test", "test"))
      val response = connection.receive(AuthenticationMD5Password(salt))
      response must equal(Some(PasswordRequired(Md5(salt))))
    }

    "auth with a cleartext password" in {
      val connection = new Connection()

      connection.send(StartupMessage("test", "test"))
      val response = connection.receive(AuthenticationCleartextPassword())
      response must equal(Some(PasswordRequired(ClearText)))

      connection.send(PasswordMessage("stub"))
      val response2 = connection.receive(AuthenticationOk())
      response2 must equal(None)

      val response3 = connection.receive(ReadyForQuery('I'))
      response3 must equal(Some(AuthenticatedResponse(Map(), -1, -1)))
    }

    "auth with an md5 password" in {
      val connection = new Connection()

      connection.send(StartupMessage("test", "test"))
      val response = connection.receive(AuthenticationMD5Password(salt))
      response must equal(Some(PasswordRequired(Md5(salt))))

      connection.send(PasswordMessage("stub"))
      val response2 = connection.receive(AuthenticationOk())
      response2 must equal(None)

      val response3 = connection.receive(ReadyForQuery('I'))
      response3 must equal(Some(AuthenticatedResponse(Map(), -1, -1)))
    }
  }
}