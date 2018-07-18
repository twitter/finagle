package com.twitter.finagle.mysql

import com.twitter.finagle._
import com.twitter.util.Future

object RollbackFactory {
  private val RollbackQuery = QueryRequest("ROLLBACK")

  private[finagle] def module: Stackable[ServiceFactory[Request, Result]] =
    new Stack.Module0[ServiceFactory[Request, Result]] {
      val role = Stack.Role("RollbackFactory")
      val description = "Installs a rollback factory in the stack"
      def make(next: ServiceFactory[Request, Result]): ServiceFactory[Request, Result] =
        new RollbackFactory(next)
    }
}

/**
 * A helper `ServiceFactory` that ensures a ROLLBACK statement is issued each time a service is
 * checkout of the pool. This exists to prevent a situation where an unfinished transaction
 * has been written to the wire, the service has been released back into the pool, the same service
 * is again checked out of the pool, and a statement that causes an implicit commit is issued.
 * This could cause an undesirable state where the service believes a transaction failed while in
 * fact it partially succeeded.
 *
 * {{{
 *  import com.twitter.finagle.Mysql
 *
 *  val client = Mysql.client
 *   .withCredentials("<user>", "<password>")
 *   .withDatabase("test")
 *   .newClient("127.0.0.1:3306")
 *
 *  val rollbackClient = new RollbackFactory(client)
 *
 *  val result = rollbackClient().flatMap { service =>
 *    // ROLLBACK is issued just before this
 *    service(QueryResult("SELECT ....")).map {
 *      ...
 *    }.ensure {
 *      service.close()
 *    }
 *  }
 * }}}
 *
 * @see https://dev.mysql.com/doc/en/implicit-commit.html
 */
final class RollbackFactory(client: ServiceFactory[Request, Result]) extends ServiceFactoryProxy(client) {
  import RollbackFactory._

  override def apply(conn: ClientConnection): Future[Service[Request, Result]] = {
    super.apply(conn).flatMap { svc =>
      svc(RollbackQuery).map { _ => svc }
    }
  }
}
