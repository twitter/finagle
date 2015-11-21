package com.twitter.finagle.client

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle._
import com.twitter.finagle.dispatch.SerialClientDispatcher
import com.twitter.finagle.service.{Backoff, FailureAccrualFactory}
import com.twitter.finagle.service.exp.FailureAccrualPolicy
import com.twitter.finagle.transport.{QueueTransport, Transport}
import com.twitter.util.{Await, Future, MockTimer, Time, Var, Closable, Return}
import com.twitter.util.TimeConversions.intToTimeableNumber
import com.twitter.finagle.stats.{StatsReceiver, InMemoryStatsReceiver}
import com.twitter.finagle.util.DefaultLogger
import com.twitter.finagle.util.InetSocketAddressUtil.unconnected
import java.net.SocketAddress
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.junit.{JUnitRunner, AssertionsForJUnit}

@RunWith(classOf[JUnitRunner])
class DefaultClientTest extends FunSuite with Eventually with IntegrationPatience with AssertionsForJUnit {
  trait StatsReceiverHelper {
    val statsReceiver = new InMemoryStatsReceiver()
  }

  trait QueueTransportHelper {
    val qIn = new AsyncQueue[Int]()
    val qOut = new AsyncQueue[Int]()

    val transporter: (SocketAddress, StatsReceiver) => Future[Transport[Int, Int]] = {
      case (_, _) =>
        Future.value(new QueueTransport(qIn, qOut))
    }
  }

  trait DispatcherHelper {
    val dispatcher: Transport[Int, Int] => Service[Int, Int]
  }

  trait SourcedExceptionDispatcherHelper extends DispatcherHelper {
    val dispatcher: Transport[Int, Int] => Service[Int, Int] = { _ =>
      Service.mk { _ =>
        throw new SourcedException {}
      }
    }
  }

  trait ServiceHelper { self: QueueTransportHelper with DispatcherHelper =>
    val endPointer = Bridge[Int, Int, Int, Int](transporter, dispatcher)
    val name = "name"
    val socket = new SocketAddress() {}
    val client: Client[Int, Int]
    lazy val service: Service[Int, Int] = client.newService(Name.bound(socket), name)
  }

  trait BaseClientHelper extends ServiceHelper { self: QueueTransportHelper with DispatcherHelper =>
    val client: Client[Int, Int] = DefaultClient[Int, Int](name, endPointer)
  }

  trait SourcedExceptionHelper extends QueueTransportHelper
    with SourcedExceptionDispatcherHelper
    with BaseClientHelper

  class DefaultClientHelper extends QueueTransportHelper
    with SerialDispatcherHelper
    with BaseClientHelper

  test("DefaultClient should successfully add sourcedexception") {
    new SourcedExceptionHelper {
      val f = service(3)
      qOut.offer(3)
      val e = intercept[SourcedException] {
        Await.result(f)
      }
      assert(e.serviceName == name)
    }
  }

  trait TimingHelper {
    val timer = new MockTimer()
  }

  trait SerialDispatcherHelper extends DispatcherHelper {
    val dispatcher: Transport[Int, Int] => Service[Int, Int] =
      new SerialClientDispatcher(_)
  }

  trait TimeoutHelper extends TimingHelper
    with QueueTransportHelper
    with SerialDispatcherHelper
    with ServiceHelper {
    val pool = new DefaultPool[Int, Int](0, 1, timer = timer) // pool of size 1
  }

  test("DefaultClient should time out on dispatch, not on queueing") {
    val rTimeout = 1.second

    new TimeoutHelper {
      val client = DefaultClient(
        name,
        endPointer,
        pool = pool,
        requestTimeout = rTimeout,
        timer = timer
      )

      Time.withCurrentTimeFrozen { control =>
        val f1 = service(3) // has a connection
        val f2 = service(4)  // is queued

        assert(!f1.isDefined)
        assert(!f2.isDefined)

        control.advance(rTimeout)
        timer.tick()
        assert(f1.isDefined) // times out
        intercept[IndividualRequestTimeoutException] {
          Await.result(f1)
        }
        assert(!f2.isDefined)
        // f2 is now moved from the queue to dispatched

        control.advance(rTimeout)
        timer.tick()
        assert(f2.isDefined) // times out
        intercept[IndividualRequestTimeoutException] {
          Await.result(f2)
        }
      }
    }
  }

  trait StatsHelper extends TimingHelper
    with QueueTransportHelper
    with SerialDispatcherHelper
    with StatsReceiverHelper
    with ServiceHelper {

    val pool = new DefaultPool[Int, Int](0, 1, timer = timer) // pool of size 1
    val client = new DefaultClient[Int, Int](
      name,
      endPointer,
      pool = pool,
      timer = timer,
      statsReceiver = statsReceiver
    )
  }

  test("DefaultClient statsfilter should time stats on dispatch, not on queueing") {
    val dur = 1.second

    new StatsHelper {
      Time.withCurrentTimeFrozen { control =>
        val f1 = service(3)
        val f2 = service(4)

        control.advance(dur)
        timer.tick()
        qOut.offer(3)
        Await.result(f1)

        control.advance(dur)
        timer.tick()
        qOut.offer(4)
        Await.result(f2)
      }

      assert(statsReceiver.stats(Seq(name, "request_latency_ms")) ==
        (Seq(dur, dur) map (_.inMillis)))
    }
  }

  test("Services should be able to be closed even if they can't be resolved") {
    new DefaultClientHelper {
      val dest = Name.Bound.singleton(Var.value(Addr.Pending))
      val svc = client.newService(dest, "test")
      val f = svc.close()
      eventually { assert(f.isDefined) }
      assert(Await.result(f) == ())
    }
  }

  test("Bound Vars should be closable") {
    new DefaultClientHelper {
      @volatile var closed = false

      val dest = Name.Bound.singleton(Var.async(Addr.Bound(Seq.empty[SocketAddress]: _*)) { _ =>
        Closable.make { _ =>
          closed = true
          Future.Done
        }
      })
      val svc = client.newService(dest, "test")
      assert(!closed, "client closed too early")
      val f = svc.close()
      eventually { assert(f.poll == Some(Return(()))) }
      assert(closed, "client not closed")
    }
  }

  class FailureAccrualException extends Exception

  trait FailureAccuralDispatchHelper extends DispatcherHelper {
    var initialFailures: Int = 5 // default failure accrual

    val dispatcher: Transport[Int, Int] => Service[Int, Int] = { _ =>
      Service.mk { _ =>
        initialFailures -= 1
        if (initialFailures >= 0)
          Future.exception(new FailureAccrualException)
        else
          Future.value(3)
      }
    }
  }

  trait DefaultFailureAccrualHelper extends TimingHelper
    with QueueTransportHelper
    with FailureAccuralDispatchHelper
    with StatsReceiverHelper
    with ServiceHelper {

    val pool = new DefaultPool[Int, Int](0, 1, timer = timer) // pool of size 1

    val client = new DefaultClient[Int, Int](
      name,
      endPointer,
      pool = pool,
      timer = timer,
      statsReceiver = statsReceiver
    )
  }

  test("DefaultClient should handle failureAccrual default") {
    new DefaultFailureAccrualHelper {
      0 until 10 foreach { service(_) }
      assert(statsReceiver.counters(Seq("failure_accrual", "removals")) == 1)
    }
  }

  test("DefaultClient should handle passed-in failure accrual") {
    new DefaultFailureAccrualHelper {
      initialFailures = 10
      override val client = new DefaultClient[Int, Int](
        name,
        endPointer,
        pool = pool,
        timer = timer,
        statsReceiver = statsReceiver,
        failureAccrual = { factory: ServiceFactory[Int, Int] =>
          FailureAccrualFactory.wrapper(statsReceiver, FailureAccrualPolicy.consecutiveFailures(6, Backoff.const(3.seconds)), name, DefaultLogger, unconnected)(timer) andThen factory
        }
      )

      Time.withCurrentTimeFrozen { control =>
        0 until 10 foreach { service(_) }
        assert(statsReceiver.counters(Seq("failure_accrual", "removals")) == 1)
        control.advance(4.seconds)
        timer.tick()
        assert(statsReceiver.counters.get(Seq("failure_accrual", "revivals")) == None)
      }
    }
  }
}
