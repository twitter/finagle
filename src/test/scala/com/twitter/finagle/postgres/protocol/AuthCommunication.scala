package com.twitter.finagle.postgres.protocol

import com.twitter.finagle.postgres.messages._
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.mutable.Specification

@RunWith(classOf[JUnitRunner])
class AuthCommunication extends Specification with ConnectionSpec {
  val salt = Array[Byte](1, 2, 3)

  "Postgres client" should {
    "Connect to client " inConnection {
      send(StartupMessage("test", "test"))
      receive(AuthenticationOk())
      response === None
      receive(ReadyForQuery('I'))
      response === Some(AuthenticatedResponse(Map(), -1, -1))
    }

    "Return error during failed connection" inConnection {
      send(StartupMessage("test", "test"))
      receive(ErrorResponse())
      response === Some(Error(None))
    }
    "Request clear text password" inConnection {
      send(StartupMessage("test", "test"))
      receive(AuthenticationCleartextPassword())
      response === Some(PasswordRequired(ClearText))
    }
    "Request md5 password" inConnection {

      send(StartupMessage("test", "test"))
      receive(AuthenticationMD5Password(salt))
      response === Some(PasswordRequired(Md5(salt)))

    }
    "Auth with cleartext password" inConnection {

      send(StartupMessage("test", "test"))
      receive(AuthenticationCleartextPassword())
      response === Some(PasswordRequired(ClearText))

      send(PasswordMessage("stub"))
      receive(AuthenticationOk())
      response === None
      receive(ReadyForQuery('I'))
      response === Some(AuthenticatedResponse(Map(), -1, -1))


    }
    "Auth with md5 password" inConnection {

      send(StartupMessage("test", "test"))
      receive(AuthenticationMD5Password(salt))
      response === Some(PasswordRequired(Md5(salt)))

      send(PasswordMessage("stub"))
      receive(AuthenticationOk())
      response === None
      receive(ReadyForQuery('I'))
      response === Some(AuthenticatedResponse(Map(), -1, -1))


    }

  }
}