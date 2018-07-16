package com.twitter.finagle.mysql

import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.util.{Await, Future}
import com.twitter.conversions.time._
import org.scalatest.FunSuite

class RollbackFactoryTest extends FunSuite {

  test("Rollback Client issues a rollback statement on checkout") {
    var requests: Seq[Request] = Seq.empty
    val client = ServiceFactory.const[Request, Result](Service.mk[Request, Result] { req: Request =>
      requests = requests :+ req
      Future.value(EOF(0, ServerStatus(0)))
    })

    val rollbackClient = new RollbackFactory(client)

    Await.ready(rollbackClient().flatMap { svc =>
      for {
        _ <- svc(QueryRequest("1"))
        _ <- svc(QueryRequest("2"))
      } {
        svc.close()
      }
    }, 5.seconds)

    Await.ready(rollbackClient().flatMap { svc =>
      svc(QueryRequest("3")).ensure { svc.close() }
    }, 5.seconds)

    val expected = Seq(
      "ROLLBACK",
      "1",
      "2",
      "ROLLBACK",
      "3"
    ).map(QueryRequest)

    assert(requests == expected)
  }
}
