package com.twitter.finagle.http2.transport.client

import com.twitter.finagle.client.EndpointerModule
import com.twitter.finagle.client.Transporter.EndpointAddr
import com.twitter.finagle.http.Request
import com.twitter.finagle.http.Response
import com.twitter.finagle.http.{Status => HttpStatus}
import com.twitter.finagle.http2.transport.client.H2Pool.OnH2Service
import com.twitter.finagle.Address
import com.twitter.finagle.ClientConnection
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.Stack
import com.twitter.finagle.Status
import com.twitter.util.Await
import com.twitter.util.Awaitable
import com.twitter.util.Closable
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.Time
import java.util.concurrent.atomic.AtomicInteger
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class H2PoolTest extends AnyFunSuite with MockitoSugar {

  private def await[T](t: Awaitable[T]): T = Await.result(t, Duration.fromSeconds(5))

  // Mostly so we don't have to keep typing the params.
  private abstract class H2ServiceFactory extends ServiceFactory[Request, Response] {
    def close(deadline: Time): Future[Unit] = Future.Done
  }

  private abstract class Ctx {
    def params: Stack.Params = Stack.Params.empty

    lazy val serviceFactory: ServiceFactory[Request, Response] =
      stack.make(params + EndpointAddr(Address("localhost", 0)))

    def endpointer(state: OnH2Service): ServiceFactory[Request, Response]

    private[this] def stack: Stack[ServiceFactory[Request, Response]] = {
      val nil = com.twitter.finagle.stack.nilStack[Request, Response]
      (H2Pool.module +: new EndpointerModule[Request, Response](
        Nil,
        (params, _) =>
          endpointer(
            params[H2Pool.OnH2ServiceParam].onH2Service
              .getOrElse(throw new IllegalStateException("Missing the OnH2Service!")))) +: nil)
    }
  }

  test("close closes the base ServiceFactory") {
    val mockSvcFactory = mock[ServiceFactory[Request, Response]]
    when(mockSvcFactory.close()).thenReturn(Future.Done)

    val ctx = new Ctx {
      def endpointer(h2Service: OnH2Service): ServiceFactory[Request, Response] =
        mockSvcFactory
    }

    await(ctx.serviceFactory.close())
    verify(mockSvcFactory, times(1)).close()
  }

  test("closes a non-multiplex session") {
    val mockSvc = mock[Service[Request, Response]]
    when(mockSvc.close()).thenReturn(Future.Done)

    val mockSvcFactory = mock[ServiceFactory[Request, Response]]
    when(mockSvcFactory.close()).thenReturn(Future.Done)
    when(mockSvcFactory.apply()).thenReturn(Future.value(mockSvc))

    val ctx = new Ctx {
      def endpointer(onH2Service: OnH2Service): ServiceFactory[Request, Response] =
        mockSvcFactory
    }

    val svc = await(ctx.serviceFactory())
    verify(mockSvcFactory, times(1)).apply()

    await(ctx.serviceFactory.close())
    verify(mockSvc, times(0)).close()
    verify(mockSvcFactory, times(1)).close()

    // now check the service back in by closing it, and it should also get closed by the pool.
    await(svc.close())
    verify(mockSvc, times(1)).close()
  }

  test("closes a multiplex session") {
    val h1svc = mock[Service[Request, Response]]
    when(h1svc.close()).thenReturn(Future.Done)

    val h2svc = mock[Service[Request, Response]]
    when(h2svc.close()).thenReturn(Future.Done)
    when(h2svc.status).thenReturn(Status.Open)

    val endpointerCount = new AtomicInteger()

    val ctx = new Ctx {
      def endpointer(onH2Service: OnH2Service): ServiceFactory[Request, Response] =
        new H2ServiceFactory {
          def apply(conn: ClientConnection): Future[Service[Request, Response]] = {
            endpointerCount.incrementAndGet()
            onH2Service(h2svc)
            Future.value(h1svc)
          }

          def status: Status = Status.Open
        }
    }

    val svc1 = await(ctx.serviceFactory())
    // These should both be backed by h2svc.
    val svc2 = await(ctx.serviceFactory())
    val svc3 = await(ctx.serviceFactory())

    assert(endpointerCount.get == 1)

    await(ctx.serviceFactory.close())

    verify(h1svc, times(0)).close()
    verify(h2svc, times(0)).close()

    await(svc1.close())
    await(svc2.close())

    verify(h1svc, times(1)).close()
    verify(h2svc, times(0)).close()

    await(svc3.close())
    verify(h2svc, times(1)).close()
  }

  test("will dispose of a closed multiplex session and spin back up the std pool") {
    val h1svc = mock[Service[Request, Response]]
    when(h1svc.close()).thenReturn(Future.Done)
    when(h1svc.status).thenReturn(Status.Open)
    when(h1svc.apply(any(classOf[Request]))).thenReturn(Future.value(Response(HttpStatus.Ok)))

    val h2svc = mock[Service[Request, Response]]
    when(h2svc.close()).thenReturn(Future.Done)
    when(h2svc.status).thenReturn(Status.Open)
    when(h2svc.apply(any(classOf[Request]))).thenReturn(Future.value(Response(HttpStatus.Accepted)))

    val endpointerCount = new AtomicInteger()

    val ctx = new Ctx {
      def endpointer(onH2Service: OnH2Service): ServiceFactory[Request, Response] =
        new H2ServiceFactory {
          def apply(conn: ClientConnection): Future[Service[Request, Response]] = {
            endpointerCount.incrementAndGet()
            onH2Service(h2svc)
            Future.value(h1svc)
          }

          def status: Status = Status.Open
        }
    }

    val svc1 = await(ctx.serviceFactory())
    assert(HttpStatus.Ok == await(svc1(Request())).status)
    // the h2 backed svc.
    val svc2 = await(ctx.serviceFactory())
    assert(HttpStatus.Accepted == await(svc2(Request())).status)

    assert(endpointerCount.get == 1)

    // Now switch to closed and get another to triger going through the default pool again
    when(h2svc.status) thenReturn (Status.Closed)
    val svc3 = await(ctx.serviceFactory())
    assert(HttpStatus.Ok == await(svc3(Request())).status)

    assert(endpointerCount.get == 2)

    // Close everything and make sure that our sessions were closed appropriately
    await(Closable.all(svc1, svc2, svc3).close())
    verify(h2svc, times(1)).close()

    // svc1 and svc3 go back to the pool
    verify(h1svc, times(0)).close()

    // Now they're closed since the pool will be closed.
    await(ctx.serviceFactory.close())
    verify(h1svc, times(2)).close() // svc1 and svc3 now closed
  }

  test("status reflects the health of the h2 session") {
    val h1svc = mock[Service[Request, Response]]
    when(h1svc.close()).thenReturn(Future.Done)

    val h2svc = mock[Service[Request, Response]]
    when(h2svc.close()).thenReturn(Future.Done)
    when(h2svc.status).thenReturn(Status.Open)

    val ctx = new Ctx {
      def endpointer(onH2Service: OnH2Service): ServiceFactory[Request, Response] = {
        new H2ServiceFactory {
          def apply(conn: ClientConnection): Future[Service[Request, Response]] = {
            onH2Service(h2svc)
            Future.value(h1svc)
          }

          def status: Status = Status.Open
        }
      }
    }

    val svc1 = await(ctx.serviceFactory()) // triggers upgrade.
    // the h2 backed svc.
    val svc2 = await(ctx.serviceFactory())

    assert(ctx.serviceFactory.status == Status.Open)
    assert(svc2.status == Status.Open)

    when(h2svc.status).thenReturn(Status.Busy)
    assert(ctx.serviceFactory.status == Status.Busy)
    assert(svc2.status == Status.Busy)

    when(h2svc.status).thenReturn(Status.Closed)
    assert(svc2.status == Status.Closed)
    // If the H2 session is closed, we use the default pool status.
    assert(ctx.serviceFactory.status == Status.Open)
  }
}
