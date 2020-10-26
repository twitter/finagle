package com.twitter.finagle.postgresql

import com.twitter.finagle.Stack

object Params {
  case class Credentials(username: String, password: Option[String])
  object Credentials {
    implicit val param: Stack.Param[Credentials] = new Stack.Param[Credentials] {
      override def show(p: Credentials): Seq[(String, () => String)] =
        // do not show the password for security reasons
        Seq(("username", () => p.username))

      override def default: Credentials = ???
    }
  }

  case class Database(name: Option[String])
  object Database {
    implicit val param: Stack.Param[Database] = Stack.Param(Database(None))
  }
}
