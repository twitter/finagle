package com.twitter.finagle.postgresql

import com.twitter.finagle.Stack
import com.twitter.util.Duration
import com.twitter.util.tunable.Tunable

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

  final case class StatementTimeout(timeout: Tunable[Duration]) {
    def this(timeout: Duration) = this(Tunable.const("StatementTimeout", timeout))
  }

  object StatementTimeout {
    def apply(timeout: Duration): StatementTimeout = new StatementTimeout(timeout)

    implicit val param: Stack.Param[StatementTimeout] =
      Stack.Param(StatementTimeout(Tunable.none[Duration]))
  }

  final case class ConnectionInitializationCommands(commands: Seq[String])

  object ConnectionInitializationCommands {
    implicit val param: Stack.Param[ConnectionInitializationCommands] =
      Stack.Param(ConnectionInitializationCommands(Nil))
  }

  final case class SessionDefaults(defaults: Map[String, String])

  object SessionDefaults {
    implicit val param: Stack.Param[SessionDefaults] =
      Stack.Param(SessionDefaults(Map.empty))
  }

}
