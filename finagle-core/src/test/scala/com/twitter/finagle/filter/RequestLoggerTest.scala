package com.twitter.finagle.filter

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.stack.Endpoint
import com.twitter.finagle.stack.nilStack
import com.twitter.logging.Level
import com.twitter.logging.Logger
import com.twitter.util.Await
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.Stopwatch
import com.twitter.util.Time
import java.util.concurrent.atomic.AtomicInteger
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.mockito.ArgumentMatchers.anyObject
import org.mockito.ArgumentMatchers.contains
import org.scalatest.BeforeAndAfter
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class RequestLoggerTest extends AnyFunSuite with BeforeAndAfter with MockitoSugar {

  private[this] val roleName = "RequestLoggerTest"

  test("StackTransformer") {
    val mockLog = mock[Logger]
    when(mockLog.isLoggable(Level.TRACE)).thenReturn(true)

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

      val transformer = RequestLogger.newStackTransformer(mockLog, "aLabel", Stopwatch.timeNanos)
      val stack = transformer(builder.result ++ Stack.leaf(Endpoint, endpoint))
      val svcFac = stack.make(Stack.Params.empty)
      val svc = Await.result(svcFac(ClientConnection.nil), 5.seconds)

      // validate the result is correct.
      assert(18 == Await.result(svc(9), 5.seconds))

      // validate each module actually ran
      assert(1 == module1Count.get())
      assert(1 == module2Count.get())

      // validate we got begin/end logs for all the modules.
      val names = Seq("module0", "module1", "module2", Endpoint.name)
      names.foreach { name =>
        verify(mockLog, times(1)).trace(contains(s"aLabel $name begin"), anyObject())
        verify(mockLog, times(1)).trace(contains(s"aLabel $name end cumulative async"), anyObject())
      }

      // validate the delays are accounted for in the right modules.
      verify(mockLog, times(1))
        .trace(contains(s"${Endpoint.name} end cumulative sync elapsed 9 us"), anyObject())
      verify(mockLog, times(1))
        .trace(contains("aLabel module2 end cumulative sync elapsed 9 us"), anyObject())
      verify(mockLog, times(1))
        .trace(contains("aLabel module1 end cumulative sync elapsed 209 us"), anyObject())
      verify(mockLog, times(1))
        .trace(contains("aLabel module0 end cumulative sync elapsed 259 us"), anyObject())
    }
  }

}
