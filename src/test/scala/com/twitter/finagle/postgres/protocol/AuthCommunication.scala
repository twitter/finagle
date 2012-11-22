package com.twitter.finagle.postgres.protocol

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.mutable.Specification

@RunWith(classOf[JUnitRunner])
class AuthCommunication extends Specification {
  "Postgres client" should {
    "Connect to client " in {
      val connection = new Connection()
      connection.send(StartupMessage("test", "test"))
      val ok = connection.receive(AuthenticationOk())
      ok === None
      val ready = connection.receive(ReadyForQuery('I'))
      ready === Some(AuthenticatedResponse(Map(), -1, -1))
    }
    "Return error during failed connection" in {
      val connection = new Connection()
      connection.send(StartupMessage("test", "test"))
      val error = connection.receive(ErrorResponse())
      error === Some(Error(None))
    }
    "Request clear text password" in {
      val connection = new Connection()
      connection.send(StartupMessage("test", "test"))
      val ok = connection.receive(AuthenticationCleartextPassword())
      ok === Some(PasswordRequired(ClearText))
    }
    "Request md5 password" in {
      val connection = new Connection()
      connection.send(StartupMessage("test", "test"))
      val salt = Array[Byte](1, 2, 3)
      val ok = connection.receive(AuthenticationMD5Password(salt))
      ok === Some(PasswordRequired(Md5(salt)))
    }
    "Auth with cleartext password" in {
      val connection = new Connection()
      connection.send(StartupMessage("test", "test"))
      val ok = connection.receive(AuthenticationCleartextPassword())
      ok === Some(PasswordRequired(ClearText))

      connection.send(PasswordMessage("stub"))
      val pass = connection.receive(AuthenticationOk())
      pass === None

      val ready = connection.receive(ReadyForQuery('I'))
      ready === Some(AuthenticatedResponse(Map(), -1, -1))
    }
    "Auth with md5 password" in {
      val connection = new Connection()
      connection.send(StartupMessage("test", "test"))
      val salt = Array[Byte](1, 2, 3)
      val ok = connection.receive(AuthenticationMD5Password(salt))
      ok === Some(PasswordRequired(Md5(salt)))

      connection.send(PasswordMessage("stub"))
      val pass = connection.receive(AuthenticationOk())
      pass === None

      val ready = connection.receive(ReadyForQuery('I'))
      ready === Some(AuthenticatedResponse(Map(), -1, -1))
    }
  }
}