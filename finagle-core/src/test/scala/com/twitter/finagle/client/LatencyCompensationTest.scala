package com.twitter.finagle.client

import com.twitter.conversions.time._
import com.twitter.finagle.service.TimeoutFilter
import com.twitter.finagle.stack.nilStack
import com.twitter.finagle.{MockTimer => _, _}
import com.twitter.util._
import org.junit.runner.RunWith
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}
import org.scalatest.FunSuite

/**
 * An end-to-end test for LatencyCompensation.
 */
@RunWith(classOf[JUnitRunner])
class LatencyCompensationTest
  extends FunSuite
  with AssertionsForJUnit
  with Eventually
  with IntegrationPatience
{
  def verifyCompensationModule(expected: Duration) =
    new Stack.Module[ServiceFactory[String, String]] {
      val role = Stack.Role("verify")
      val description = "Verify stack behavior"
      val parameters = Seq(
        implicitly[Stack.Param[LatencyCompensation.Compensator]])
      def make(prms: Stack.Params, next: Stack[ServiceFactory[String, String]]) = {
        val LatencyCompensation.Compensation(compensation) = prms[LatencyCompensation.Compensation]
        assert(expected === compensation)

        Stack.Leaf(this, ServiceFactory.const(Service.mk[String, String](Future.value)))
      }
    }

  test("Sets Compensation param") {
    val stk = new StackBuilder[ServiceFactory[String, String]](nilStack[String, String])
    stk.push(verifyCompensationModule(100.millis))
    stk.push(LatencyCompensation.module)
    stk.result.make(Stack.Params.empty + LatencyCompensation.Compensator(_ => 100.millis))
  }

  test("Defaults to zero") {
    val stk = new StackBuilder[ServiceFactory[String, String]](nilStack[String, String])
    stk.push(verifyCompensationModule(0.second))
    stk.push(LatencyCompensation.module)
    stk.result.make(Stack.Params.empty)
  }

  class Ctx {

    val timer = new MockTimer

    class TestPromise[T] extends Promise[T] {
      @volatile var interrupted: Option[Throwable] = None
      setInterruptHandler { case exc =>
        interrupted = Some(exc)
      }
    }
    val receive, respond = new TestPromise[String]

    def awaitReceipt(): Unit =
      // Spin on this so we don't Await into a new thread, which breaks the clock.
      while (receive.poll == None) {}


    val service = Service.mk[String, String] { in =>
      receive.setValue(in)
      respond
    }

    var baseTimeout = 1.second
    var metadata: Addr.Metadata = Addr.Metadata.empty
    var compensator: (Addr.Metadata => Duration) = { metadata =>
      metadata.get("compensation") match {
        case Some(compensation: Duration) => compensation
        case _ => Duration.Zero
      }
    }

    /*
     * N.B. connection timeout compensation is not tested
     * end-to-end-because it's tricky to cause connection latency.
     */

    def whileConnected(f: Service[String, String] => Unit): Unit = {
      val server = Echo.serve("127.1:0", service)
      val addr = Addr.Bound(Set(server.boundAddress), metadata)
      val client =
        Echo.stringClient
            .configured(TimeoutFilter.Param(baseTimeout))
            .configured(param.Timer(timer))
            .configured(LatencyCompensation.Compensator(compensator))
            .newService(Name.Bound(Var.value(addr), "id"), "label")

      try f(client)
      finally Await.result(client.close() join server.close(), 10.seconds)
    }
  }


  test("TimeoutFilter.module accomodates latency compensation") {
    new Ctx {
      metadata = Addr.Metadata("compensation" -> 2.seconds)

      Time.withCurrentTimeFrozen { clock =>
        whileConnected { client =>
          val yo = client("yo")
          assert(!yo.isDefined)

          awaitReceipt()
          assert(!yo.isDefined)

          clock.advance(2.seconds)
          timer.tick()

          respond.setValue("yo")
          eventually {
            assert(yo.isDefined)
          }
          assert(Await.result(yo, 10.seconds) === "yo")
        }
      }
    }
  }

  if (!sys.props.contains("SKIP_FLAKY")) // TRFC-325
  test("TimeoutFilter.module still times out requests when compensating") {
    new Ctx {
      metadata = Addr.Metadata("compensation" -> 2.seconds)

      Time.withCurrentTimeFrozen { clock =>
        whileConnected { client =>
          val sup = client("sup")
          assert(!sup.isDefined)
          assert(respond.interrupted === None)

          awaitReceipt()
          assert(!sup.isDefined)
          assert(respond.interrupted === None)

          clock.advance(4.seconds)
          timer.tick() // triggers the timeout

          eventually {
            assert(sup.isDefined)
          }
          assert(respond.interrupted.isDefined)
          intercept[IndividualRequestTimeoutException] {
            Await.result(sup, 10.seconds)
          }
        }
      }
    }
  }

  if (!sys.props.contains("SKIP_FLAKY")) // TRFC-325
  test("Latency compensator doesn't always add compensation") {
    new Ctx {
      Time.withCurrentTimeFrozen { clock =>
        whileConnected { client =>
          val nm = client("nm")
          assert(!nm.isDefined)
          assert(respond.interrupted === None)

          awaitReceipt()
          assert(!nm.isDefined)
          assert(respond.interrupted === None)

          clock.advance(2.seconds)
          timer.tick() // triggers the timeout

          eventually {
            assert(nm.isDefined)
          }
          assert(respond.interrupted.isDefined)
          intercept[IndividualRequestTimeoutException] {
            Await.result(nm, 10.seconds)
          }
        }
      }
    }
  }

  test("Latency compensator doesn't apply if there's no base timeout") {
    new Ctx {
      baseTimeout = Duration.Top
      metadata = Addr.Metadata("compensation" -> 2.seconds)

      Time.withCurrentTimeFrozen { clock =>
        whileConnected { client =>
          val aight = client("aight")
          assert(!aight.isDefined)
          assert(respond.interrupted === None)

          awaitReceipt()
          assert(!aight.isDefined)
          assert(respond.interrupted === None)

          clock.advance(4.seconds)
          timer.tick() // does not trigger the timeout

          respond.setValue("aight")
          eventually {
            assert(aight.isDefined)
          }
          assert(Await.result(aight, 10.seconds) === "aight")
        }
      }
    }
  }
}
