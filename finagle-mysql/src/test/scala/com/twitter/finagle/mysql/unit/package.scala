package com.twitter.finagle.exp

import com.twitter.finagle.{ClientConnection, ServiceFactory, Service}
import com.twitter.util.{Time, Future}

/**
 * Mock objects for testing.
 */
package object mysql {

  class MockService extends Service[Request, Result] {
    var requests = List[Request]()
    val resultSet = new ResultSet(fields=Seq(), rows=Seq())

    def apply(request: Request): Future[Result] = {
      requests = requests ++ List(request)
      Future.value(resultSet)
    }
  }

  class MockServiceFactory(service: Service[Request, Result]) extends ServiceFactory[Request, Result] {
    def apply(conn: ClientConnection): Future[Service[Request, Result]] = Future.value(service)
    def close(deadline: Time): Future[Unit] = Future.Unit
  }
}
