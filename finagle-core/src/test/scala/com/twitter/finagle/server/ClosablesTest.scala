package com.twitter.finagle.server

import com.twitter.conversions.DurationOps._
import com.twitter.util.{Await, Awaitable, Closable, Future, Promise, Time}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.funsuite.AnyFunSuite

class ClosablesTest extends AnyFunSuite with Eventually with IntegrationPatience {

  private def await[T](awaitable: Awaitable[T]): T = Await.result(awaitable, 5.seconds)

  private class CheckingClosable extends Closable {
    val promise = Promise[Unit]
    var deadline: Option[Time] = None

    def close(deadline: Time): Future[Unit] = {
      this.deadline = Some(deadline)
      promise
    }
  }

  test("Registered Closables will be closed by the close of the manager") {
    val manager = new Closables
    val c = new CheckingClosable
    manager.register(c)

    val t = Time.now
    val f = manager.close(t)

    assert(!f.isDefined)
    assert(c.deadline == Some(t))
    c.promise.setDone()
    await(f)
  }

  test("Closables registered after the manager is closed will be closed immediately") {
    val manager = new Closables
    val t = Time.now
    manager.close(t)

    val c = new CheckingClosable
    manager.register(c)

    assert(c.deadline == Some(t))
  }

  test("Closables can be unregistered from the manager") {
    val manager = new Closables
    val c = new CheckingClosable
    manager.register(c)
    assert(manager.unregister(c))

    assert(c.deadline == None)

    await(manager.close())

    // As its unregistered, it should not be affected by closing the manager
    assert(c.deadline == None)
  }
}
