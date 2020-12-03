package com.twitter.finagle.postgresql

import com.twitter.finagle.Stack

object Params {
  case class Credentials(username: String, password: Option[String])
  object Credentials {
    implicit val param: Stack.Param[Credentials] = new Stack.Param[Credentials] {
      override def show(p: Credentials): Seq[(String, () => String)] =
        // do not show the password for security reasons
        Seq(("username", () => p.username))

      override def default: Credentials = Credentials("postgres", None)
    }
  }

  case class Database(name: Option[String])
  object Database {
    implicit val param: Stack.Param[Database] = Stack.Param(Database(None))
  }

  /**
   * A class eligible for configuring the maximum number of prepare
   * statements.  After creating `num` prepare statements, we'll start purging
   * old ones.
   */
  case class MaxConcurrentPrepareStatements(num: Int) {
    assert(num > 0, s"$num must be positive")

    def mk(): (MaxConcurrentPrepareStatements, Stack.Param[MaxConcurrentPrepareStatements]) =
      (this, MaxConcurrentPrepareStatements.param)
  }

  object MaxConcurrentPrepareStatements {
    implicit val param: Stack.Param[MaxConcurrentPrepareStatements] =
      Stack.Param(MaxConcurrentPrepareStatements(20))
  }

}
