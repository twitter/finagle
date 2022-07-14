package com.twitter.finagle.mysql

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.ChannelClosedException
import com.twitter.finagle.ClientConnection
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.Status
import com.twitter.util.Await
import com.twitter.util.Awaitable
import com.twitter.util.Future
import com.twitter.util.Time
import org.scalatest.funsuite.AnyFunSuite

class RollbackFactoryTest extends AnyFunSuite {

  private[this] def await[T](t: Awaitable[T]): T = Await.result(t, 5.seconds)

  test("RollbackFactory issues a rollback for each connection pool transaction") {
    var requests: Seq[Request] = Seq.empty
    val client = ServiceFactory.const[Request, Result](Service.mk[Request, Result] { req: Request =>
      requests = requests :+ req
      Future.value(EOF(0, ServerStatus(0)))
    })

    val rollbackClient = new RollbackFactory(client, NullStatsReceiver)

    await(rollbackClient().flatMap { svc =>
      for {
        _ <- svc(QueryRequest("1"))
        _ <- svc(QueryRequest("2"))
      } {
        svc.close()
      }
    })

    await(rollbackClient().flatMap { svc => svc(QueryRequest("3")).ensure { svc.close() } })

    val expected = Seq(
      "1",
      "2",
      "ROLLBACK",
      "3",
      "ROLLBACK"
    ).map(QueryRequest)

    assert(requests == expected)
  }

  test("close is called on underlying service when rollback succeeds") {
    var closeCalled = false

    val client = new ServiceFactory[Request, Result] {
      private[this] val svc = new Service[Request, Result] {
        def apply(req: Request) = Future.value(EOF(0, ServerStatus(0)))
        override def close(when: Time) = {
          closeCalled = true
          Future.Done
        }
      }
      def apply(c: ClientConnection) = Future.value(svc)
      def close(deadline: Time): Future[Unit] = svc.close(deadline)
      def status: Status = svc.status
    }

    val rollbackClient = new RollbackFactory(client, NullStatsReceiver)

    await(rollbackClient().flatMap { svc => svc(QueryRequest("1")).ensure { svc.close() } })
    assert(closeCalled)
  }

  test("poison request is sent when rollback fails") {
    var requests: Seq[Request] = Seq.empty
    var closeCalled = false
    val client = new ServiceFactory[Request, Result] {
      private[this] val svc = new Service[Request, Result] {
        def apply(req: Request) = req match {
          case QueryRequest("ROLLBACK") => Future.exception(new Exception("boom"))
          case _ =>
            requests = requests :+ req
            Future.value(EOF(0, ServerStatus(0)))
        }
        override def close(when: Time) = {
          closeCalled = true
          Future.Done
        }
      }
      def apply(c: ClientConnection) = Future.value(svc)
      def close(deadline: Time): Future[Unit] = svc.close(deadline)
      def status: Status = svc.status
    }

    val rollbackClient = new RollbackFactory(client, NullStatsReceiver)

    await(rollbackClient().flatMap { svc => svc(QueryRequest("1")).ensure { svc.close() } })

    assert(requests == Seq(QueryRequest("1"), PoisonConnectionRequest))
    assert(closeCalled)
  }

  test("poison request is sent when rollback fails with ChannelClosedException") {
    var requests: Seq[Request] = Seq.empty
    var closeCalled = false
    val client: ServiceFactory[Request, Result] = new ServiceFactory[Request, Result] {
      private[this] val svc: Service[Request, Result] = new Service[Request, Result] {
        def apply(req: Request): Future[EOF] = req match {
          case QueryRequest("ROLLBACK") => Future.exception(new ChannelClosedException())
          case _ =>
            requests = requests :+ req
            Future.value(EOF(0, ServerStatus(0)))
        }
        override def close(when: Time): Future[Unit] = {
          closeCalled = true
          Future.Done
        }
      }
      def apply(c: ClientConnection): Future[Service[Request, Result]] = Future.value(svc)
      def close(deadline: Time): Future[Unit] = svc.close(deadline)
      def status: Status = svc.status
    }

    val rollbackClient = new RollbackFactory(client, NullStatsReceiver)

    await(rollbackClient().flatMap { svc => svc(QueryRequest("1")).ensure { svc.close() } })

    assert(requests == Seq(QueryRequest("1"), PoisonConnectionRequest))
    assert(closeCalled)
  }

  test("the rollback query is omitted if the underlying service already has status closed") {
    var requests: Seq[Request] = Seq.empty
    var closeCalled = false
    val client: ServiceFactory[Request, Result] = new ServiceFactory[Request, Result] {
      private[this] val svc: Service[Request, Result] = new Service[Request, Result] {
        def apply(req: Request): Future[EOF] = {
          requests = requests :+ req
          Future.exception(new ChannelClosedException())
        }

        override def close(when: Time): Future[Unit] = {
          closeCalled = true
          Future.Done
        }

        override def status: Status = Status.Closed
      }
      def apply(c: ClientConnection): Future[Service[Request, Result]] = Future.value(svc)
      def close(deadline: Time): Future[Unit] = svc.close(deadline)
      def status: Status = svc.status
    }

    val rollbackClient = new RollbackFactory(client, NullStatsReceiver)

    await(rollbackClient().flatMap(_.close()))

    assert(requests == Seq(PoisonConnectionRequest))
    assert(closeCalled)
  }
}
