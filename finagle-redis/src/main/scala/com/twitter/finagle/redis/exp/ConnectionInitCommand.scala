package com.twitter.finagle.redis.exp

import com.twitter.finagle._
import com.twitter.finagle.redis.ServerError
import com.twitter.finagle.redis.param.{Database, Password}
import com.twitter.finagle.redis.protocol._
import com.twitter.util.Future

/**
 * A stack module that executes commands when it creates a new connection.
 */
object ConnectionInitCommand {

  val Role: Stack.Role = Stack.Role("RedisInitialCommand")

  def module: Stackable[ServiceFactory[Command, Reply]] =
    new Stack.Module2[Database, Password, ServiceFactory[Command, Reply]] {
      val role: Stack.Role = Role
      val description: String = "Manage redis connections"

      def make(
        db: Database,
        password: Password,
        next: ServiceFactory[Command, Reply]
      ): ServiceFactory[Command, Reply] = {
        for {
          service <- next
          authed <- apply(service, password.code, Auth.apply)
          selected <- apply(authed, db.index, Select.apply)
        } yield selected
      }
    }

  private def apply[A](
    service: Service[Command, Reply],
    param: Option[A],
    command: A => Command
  ): Future[Service[Command, Reply]] = {
    param match {
      case Some(p) =>
        service(command(p)).flatMap {
          case StatusReply(_) => Future.value(service)
          case ErrorReply(msg) =>
            service.close().flatMap { _ => Future.exception(ServerError(msg)) }
          case r =>
            Future.exception(new IllegalStateException(s"Unsupported response to a modify='$r'"))
        }
      case None => Future.value(service)
    }
  }
}
