package com.twitter.finagle.client

import com.twitter.finagle.{
  FactoryToService,
  Name,
  Service,
  ServiceClosedException,
  ServiceFactory,
  Stack,
  Status
}
import com.twitter.util.{Await, Future}
import com.twitter.conversions.DurationOps._
import org.scalatest.funsuite.AnyFunSuite

class MethodPoolTest extends AnyFunSuite {

  private class EchoClient extends StackBasedClient[String, String] {
    def transformed(t: Stack.Transformer): StackBasedClient[String, String] = this
    def newService(dest: Name, label: String): Service[String, String] =
      new FactoryToService(newClient(dest, label))

    def newClient(
      dest: Name,
      label: String
    ): ServiceFactory[String, String] = ServiceFactory.const(new Service[String, String] {
      def apply(req: String): Future[String] = Future.value(req)
    })

    def params: Stack.Params = Stack.Params.empty

    def withParams(ps: Stack.Params): StackBasedClient[String, String] = this
  }

  private def pool(): MethodPool[String, String] =
    new MethodPool[String, String](new EchoClient, Name.empty, "")

  test("returns closed service if never opened") {
    val p = pool()
    assert(p.get.status == Status.Closed)
    assert(p.get.close() eq Future.Done)
    assert(p.close() eq Future.Done)
    assertThrows[ServiceClosedException](Await.result(p.get(""), 2.seconds))
  }

  test("materializes the stack once") {
    val p = pool()
    p.materialize(Stack.Params.empty)
    p.open()
    val first = p.get

    p.materialize(Stack.Params.empty)
    p.open()
    val second = p.get

    assert(first eq second)
  }

  test("returns the same close promise for multiple close requests") {
    val p = pool()
    p.materialize(Stack.Params.empty)
    p.open()
    p.open()

    val first = p.close()
    assert(!first.isDefined)

    val second = p.close()
    assert(first eq second)
  }

  test("can close and re-open the shared service") {
    val p = pool()
    p.materialize(Stack.Params.empty)
    p.open()

    val first = p.get
    Await.result(p.close(), 2.second)

    p.materialize(Stack.Params.empty)
    p.open()
    val second = p.get
    Await.result(p.close(), 2.second)

    assert(!(first eq second))
  }

  test("materialize is a no-op if the client is already materialized") {
    val p = pool()
    p.materialize(Stack.Params.empty)
    p.materialize(Stack.Params.empty)
    p.materialize(Stack.Params.empty)

    p.open()

    Await.result(p.close(), 2.second)
  }
}
