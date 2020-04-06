package com.twitter.finagle.mysql

import com.twitter.finagle._
import com.twitter.finagle.mysql.param.ConnectionInitRequest
import com.twitter.util._

/**
  * A stack module that executes a request when create a connection.
  */
object ConnectionInitSql {
  val Role: Stack.Role = Stack.Role("ConnectionInitSql")

  private[finagle] def module: Stackable[ServiceFactory[Request, Result]] =
    new Stack.Module1[ConnectionInitRequest, ServiceFactory[Request, Result]] {
      val role: Stack.Role = Role
      val description: String = "Installs a connection init sql module in the stack"

      def make(in: ConnectionInitRequest, next: ServiceFactory[Request, Result]): ServiceFactory[Request, Result] = {
        in.request match {
          case Some(req) =>
            // we assume that this module added below the connection pool
            next.flatMap { service =>
              service(req).flatMap {
                case Error(code, sqlState, message) =>
                  Future.exception(ServerError(code, sqlState, message))
                case _ => Future.value(service)
              }
            }
          case None => next
        }
      }
    }
}
