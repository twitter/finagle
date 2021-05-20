package com.twitter.finagle.client

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.client.LatencyCompensation.Compensator
import com.twitter.finagle.service.TimeoutFilter
import com.twitter.finagle.stack.nilStack
import com.twitter.finagle._
import com.twitter.finagle.client.utils.StringClient
import com.twitter.finagle.server.utils.StringServer
import com.twitter.util._
import java.net.InetSocketAddress
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatestplus.junit.AssertionsForJUnit
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

/**
 * An end-to-end test for LatencyCompensation.
 */
class LatencyCompensationTest
    extends AnyFunSuite
    with AssertionsForJUnit
    with Eventually
    with BeforeAndAfterEach
    with IntegrationPatience {
  def verifyCompensationModule(expected: Duration) =
    new Stack.Module[ServiceFactory[String, String]] {
      val role = Stack.Role("verify")
      val description = "Verify stack behavior"
      val parameters = Seq(implicitly[Stack.Param[LatencyCompensation.Compensator]])
      def make(prms: Stack.Params, next: Stack[ServiceFactory[String, String]]) = {
        val LatencyCompensation.Compensation(compensation) = prms[LatencyCompensation.Compensation]
        assert(expected == compensation)

        Stack.leaf(this, ServiceFactory.const(Service.mk[String, String](Future.value)))
      }
    }

  // after each test, make sure to reset the override
  override def afterEach() =
    LatencyCompensation.DefaultOverride.reset()

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

  test("Override can only be set once") {
    assert(LatencyCompensation.DefaultOverride.set(Compensator(_ => Duration.Zero)))
    assert(!LatencyCompensation.DefaultOverride.set(Compensator(_ => Duration.Zero)))
  }

  class Ctx {

    val timer = new MockTimer()

    class TestPromise[T] extends Promise[T] {
      @volatile var interrupted: Option[Throwable] = None
      setInterruptHandler {
        case exc =>
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

    lazy val baseEchoClient = StringClient.client
      .configured(TimeoutFilter.Param(baseTimeout))
      .configured(param.Timer(timer))

    lazy val compensatedEchoClient = baseEchoClient
      .configured(LatencyCompensation.Compensator(compensator))

    /*
     * N.B. connection timeout compensation is not tested
     * end-to-end-because it's tricky to cause connection latency.
     */
    def whileConnected(
      echoClient: StringClient.Client
    )(
      f: Service[String, String] => Unit
    ): Unit = {
      val server = StringServer.server.serve("127.1:0", service)
      val ia = server.boundAddress.asInstanceOf[InetSocketAddress]
      val addr = Addr.Bound(Set[Address](Address(ia)), metadata)
      val client = echoClient.newService(Name.Bound(Var.value(addr), "id"), "label")

      try f(client)
      finally Await.result(client.close() join server.close(), 10.seconds)
    }
  }

  test("TimeoutFilter.module accommodates latency compensation") {
    new Ctx {
      metadata = Addr.Metadata("compensation" -> 2.seconds)

      Time.withCurrentTimeFrozen { clock =>
        whileConnected(compensatedEchoClient) { client =>
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
          assert(Await.result(yo, 10.seconds) == "yo")
        }
      }
    }
  }

  test(
    "TimeoutFilter.module accommodates configured latency compensation even when default override is set"
  ) {
    new Ctx {
      // set a compensation to 0 which should cause a failure if the caller does not
      // explicitly .configure the client with a compensation parameter.
      LatencyCompensation.DefaultOverride.set(new Compensator(_ => Duration.Zero))

      metadata = Addr.Metadata("compensation" -> 2.seconds)

      Time.withCurrentTimeFrozen { clock =>
        // use the client which is configured with a compensation parameter
        whileConnected(compensatedEchoClient) { client =>
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
          assert(Await.result(yo, 10.seconds) == "yo")
        }
      }
    }
  }

  test("TimeoutFilter.module accommodates configured latency compensation when set by override") {
    new Ctx {
      // Do not set the .configured param for LatencyCompensation. Instead override the default
      // compensation to 2 seconds which will make this succeed.
      LatencyCompensation.DefaultOverride.set(new Compensator(_ => 2.seconds))

      Time.withCurrentTimeFrozen { clock =>
        whileConnected(baseEchoClient) { client =>
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
          assert(Await.result(yo, 10.seconds) == "yo")
        }
      }
    }
  }

  if (!sys.props.contains("SKIP_FLAKY")) // TRFC-325
    test("TimeoutFilter.module still times out requests when compensating") {
      new Ctx {
        metadata = Addr.Metadata("compensation" -> 2.seconds)

        Time.withCurrentTimeFrozen { clock =>
          whileConnected(compensatedEchoClient) { client =>
            val sup = client("sup")
            assert(!sup.isDefined)
            assert(respond.interrupted == None)

            awaitReceipt()
            assert(!sup.isDefined)
            assert(respond.interrupted == None)

            clock.advance(6.seconds)
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

  test("Latency compensator doesn't always add compensation") {
    new Ctx {
      Time.withCurrentTimeFrozen { clock =>
        whileConnected(compensatedEchoClient) { client =>
          val nm = client("nm")
          assert(!nm.isDefined)
          assert(respond.interrupted.isEmpty)

          awaitReceipt()
          assert(!nm.isDefined)
          assert(respond.interrupted.isEmpty)

          clock.advance(2.seconds)
          timer.tick() // triggers the timeout

          eventually {
            assert(nm.isDefined)
          }
          eventually {
            assert(respond.interrupted.isDefined)
          }
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
        whileConnected(compensatedEchoClient) { client =>
          val aight = client("aight")
          assert(!aight.isDefined)
          assert(respond.interrupted == None)

          awaitReceipt()
          assert(!aight.isDefined)
          assert(respond.interrupted == None)

          clock.advance(4.seconds)
          timer.tick() // does not trigger the timeout

          respond.setValue("aight")
          eventually {
            assert(aight.isDefined)
          }
          assert(Await.result(aight, 10.seconds) == "aight")
        }
      }
    }
  }
}
