package com.twitter.finagle.mysql

import com.twitter.finagle._
import com.twitter.finagle.mysql.param.ConnectionInitRequest
import com.twitter.util._

/**
 * A stack module that executes a request when create a connection.
 *
 * @note that this needs to be added at the bottom of the stack near
 *       the `prepConn` Role.
 */
object ConnectionInitSql {
  val Role: Stack.Role = Stack.Role("ConnectionInitSql")

  private[finagle] def module: Stackable[ServiceFactory[Request, Result]] =
    new Stack.Module1[ConnectionInitRequest, ServiceFactory[Request, Result]] {
      val role: Stack.Role = Role
      val description: String = "Installs a connection init sql module in the stack"

      def make(
        in: ConnectionInitRequest,
        next: ServiceFactory[Request, Result]
      ): ServiceFactory[Request, Result] = {
        in.request match {
          case Some(req) =>
            next.flatMap { service =>
              service(req).flatMap {
                case _: OK => Future.value(service)
                case Error(code, sqlState, message) =>
                  Future.exception(ServerError(code, sqlState, message))
                case r =>
                  Future.exception(new Exception(s"Unsupported response to an init request: '$r'"))
              }
            }
          case None => next
        }
      }
    }
}
