package com.twitter.finagle.filter

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.stack.{Endpoint, nilStack}
import com.twitter.logging.{BareFormatter, Level, Logger, StringHandler}
import com.twitter.util.{Await, Duration, Future, Stopwatch, Time}
import java.util.concurrent.atomic.AtomicInteger
import org.scalatest.{BeforeAndAfter, FunSuite}

class RequestLoggerTest extends FunSuite with BeforeAndAfter {

  private[this] val logger = Logger.get(RequestLogger.loggerName)
  private[this] val handler = new StringHandler(BareFormatter, None)

  private[this] val roleName = "RequestLoggerTest"
  private[this] val requestLogger =
    new RequestLogger("aLabel", roleName, Stopwatch.systemNanos)

  before {
    handler.clear()
    logger.clearHandlers()
    logger.addHandler(handler)
  }

  after {
    logger.setLevel(Level.INFO)
  }

  test("does not log at info level") {
    logger.setLevel(Level.INFO)
    assert(!requestLogger.shouldTrace)
  }

  test("logs at trace level") {
    logger.setLevel(Level.TRACE)
    assert(requestLogger.shouldTrace)
  }

  test("StackTransformer") {
    logger.setLevel(Level.TRACE)
    Time.withCurrentTimeFrozen { tc =>
      def newModule(name: String, counter: AtomicInteger, delay: Duration) =
        new Stack.Module0[ServiceFactory[Int, Int]] {
          def role: Stack.Role = Stack.Role(name)
          def description: String = role.name
          def make(next: ServiceFactory[Int, Int]): ServiceFactory[Int, Int] = {
            val f = new SimpleFilter[Int, Int] {
              def apply(request: Int, service: Service[Int, Int]): Future[Int] = {
                counter.incrementAndGet()
                tc.advance(delay)
                service(request)
              }
            }
            f.andThen(next)
          }
        }

      val endpoint = ServiceFactory.constant(Service.mk { i: Int =>
        tc.advance(i.microseconds)
        Future.value(i * 2)
      })

      val module0Count = new AtomicInteger()
      val module1Count = new AtomicInteger()
      val module2Count = new AtomicInteger()
      val builder = new StackBuilder(nilStack[Int, Int])
      // requests will traverse from 0 -> 1 -> 2 -> endpoint and then back 2 -> 1 -> 0
      builder.push(newModule("module2", module2Count, Duration.Zero))
      builder.push(newModule("module1", module1Count, 200.microseconds))
      builder.push(newModule("module0", module0Count, 50.microseconds))

      val transformer = RequestLogger.newStackTransformer("aLabel", Stopwatch.timeNanos)
      val stack = transformer(builder.result ++ Stack.Leaf(Endpoint, endpoint))
      val svcFac = stack.make(Stack.Params.empty)
      val svc = Await.result(svcFac(ClientConnection.nil), 5.seconds)

      // validate the result is correct.
      assert(18 == Await.result(svc(9), 5.seconds))

      // validate each module actually ran
      assert(1 == module1Count.get())
      assert(1 == module2Count.get())

      // validate we got begin/end logs for all the modules.
      val out = handler.get
      val names = Seq("module0", "module1", "module2", Endpoint.name)
      names.foreach { name =>
        assert(out.contains(s"aLabel $name begin"))
        assert(out.contains(s"aLabel $name end cumulative async"))
      }

      // validate the delays are accounted for in the right modules.
      assert(out.contains(s"${Endpoint.name} end cumulative sync elapsed 9 us"))
      assert(out.contains("aLabel module2 end cumulative sync elapsed 9 us"))
      assert(out.contains("aLabel module1 end cumulative sync elapsed 209 us"))
      assert(out.contains("aLabel module0 end cumulative sync elapsed 259 us"))
    }
  }

}
