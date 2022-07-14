package com.twitter.finagle.mysql

import com.twitter.finagle.Status
import com.twitter.finagle.ClientConnection
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.util.Future
import com.twitter.util.Time

/**
 * Mock objects for testing.
 */
class MockService extends Service[Request, Result] {
  var requests = List[Request]()
  val resultSet = new ResultSet(fields = Seq(), rows = Seq())

  def apply(request: Request): Future[Result] = {
    requests = requests ++ List(request)
    Future.value(resultSet)
  }
}

class MockServiceFactory(service: Service[Request, Result])
    extends ServiceFactory[Request, Result] {
  def apply(conn: ClientConnection): Future[Service[Request, Result]] = Future.value(service)
  def close(deadline: Time): Future[Unit] = Future.Unit
  def status: Status = Status.Open
}
