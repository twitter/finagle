package com.twitter.finagle.filter

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.context.{Contexts, Retries}
import com.twitter.finagle.stack.nilStack
import com.twitter.util.{Promise, Await, Future}
import org.scalatest.funsuite.AnyFunSuite

class ClearContextValueFilterTest extends AnyFunSuite {

  trait Helper {
    val setContextFilterCalled = new Promise[Unit]
    val verifyContextClearedFilterCalled = new Promise[Unit]

    val setContextFilter =
      new Stack.Module0[ServiceFactory[Unit, Unit]] {
        val role = Stack.Role("SetContext")
        val description = "set context"
        def make(next: ServiceFactory[Unit, Unit]) =
          new SimpleFilter[Unit, Unit] {
            def apply(req: Unit, service: Service[Unit, Unit]): Future[Unit] = {
              Contexts.broadcast.let(Retries, Retries(5)) {
                assert(Retries.current == Some(Retries(5)))
                setContextFilterCalled.setDone()
                service(req)
              }
            }
          }.andThen(next)
      }

    val clearContextFilter = ClearContextValueFilter.module[Unit, Unit](Retries)

    val verifyContextClearedFilter =
      new Stack.Module0[ServiceFactory[Unit, Unit]] {
        val role = Stack.Role("VerifyContextCleared")
        val description = "verify context cleared"
        def make(next: ServiceFactory[Unit, Unit]) =
          new SimpleFilter[Unit, Unit] {
            def apply(req: Unit, service: Service[Unit, Unit]): Future[Unit] = {
              assert(Retries.current == None)
              verifyContextClearedFilterCalled.setDone()
              service(req)
            }
          }.andThen(next)
      }

    val svcModule = new Stack.Module0[ServiceFactory[Unit, Unit]] {
      val role = Stack.Role("svcModule")
      val description = ""
      def make(next: ServiceFactory[Unit, Unit]) =
        ServiceFactory.const(Service.mk[Unit, Unit](Future.value))
    }
  }

  test("clears context for configured key") {
    new Helper {
      val factory = new StackBuilder[ServiceFactory[Unit, Unit]](nilStack[Unit, Unit])
        .push(svcModule)
        .push(verifyContextClearedFilter)
        .push(clearContextFilter)
        .push(setContextFilter)
        .make(Stack.Params.empty)

      val svc: Service[Unit, Unit] = Await.result(factory(), 1.second)
      Await.result(svc(()), 1.second)
      assert(setContextFilterCalled.isDefined)
      assert(verifyContextClearedFilterCalled.isDefined)
    }
  }

  test("does not blow up if key does not exist") {
    new Helper {
      val factory = new StackBuilder[ServiceFactory[Unit, Unit]](nilStack[Unit, Unit])
        .push(svcModule)
        .push(verifyContextClearedFilter)
        .push(clearContextFilter)
        .make(Stack.Params.empty)

      val svc: Service[Unit, Unit] = Await.result(factory(), 1.second)
      Await.result(svc(()), 1.second)
      assert(verifyContextClearedFilterCalled.isDefined)
    }
  }
}
