package com.twitter.finagle.postgresql

import com.twitter.finagle.postgresql.Client.Expect
import com.twitter.finagle.{Service, Status}
import com.twitter.io.Reader
import com.twitter.util.{Await, CloseOnce, Future, Time}
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

object DelayedReleaseServiceSpec {

  private class MockService(numRows: Int) extends Service[Request, Response] with CloseOnce {
    private def row(): Response.Row = IndexedSeq()

    private def rs(): Response.ResultSet = {
      val reader = Reader.fromSeq(Seq.fill(numRows)(row()))
      Response.ResultSet(IndexedSeq.empty, reader, Response.ConnectionParameters.empty)
    }

    override def apply(request: Request): Future[Response] = request match {
      case _: Request.ExecutePortal =>
        Future(rs())
      case Request.Sync =>
        Future(Response.Ready)
      case Request.Query(_) =>
        val responseReader = Reader.fromSeq(Seq.fill(2)(rs()))
        Future(Response.SimpleQueryResponse(responseReader))
      case _ => ???
    }

    override def status: Status = if (isClosed) Status.Closed else Status.Open

    override protected def closeOnce(deadline: Time): Future[Unit] = Future.Done
  }
}

class DelayedReleaseServiceSpec extends AnyWordSpec with Matchers {
  import DelayedReleaseServiceSpec._

  private def executeRequest: Request.ExecutePortal =
    Request.ExecutePortal(Response.Prepared(Types.Name.Unnamed, IndexedSeq.empty), IndexedSeq.empty)

  "DelayedReleaseService" should {
    "defer close when handling a streaming response" in {
      val svc = new MockService(1)
      val delayed = new DelayedReleaseService(svc)

      val fut = delayed(executeRequest)
        .flatMap(Expect.ResultSet)

      svc.status must be(Status.Open)

      val resp = Await.result(fut)
      val reader = resp.rows

      val closeFuture = delayed.close()
      closeFuture.isDefined must be(false)

      Await.result(reader.read())

      svc.status must be(Status.Open)

      Await.result(reader.read()) must be(None)
      Await.result(closeFuture)
      svc.status must be(Status.Closed)
    }

    "defer close when handling a multi-query response" in {
      val svc = new MockService(1)
      val delayed = new DelayedReleaseService(svc)

      val fut = delayed(Request.Query("query"))
        .flatMap(Expect.SimpleQueryResponse)

      val resp = Await.result(fut)
      val closeFuture = delayed.close()

      closeFuture.isDefined must be(false)

      val readers = Await
        .result(Reader.readAllItems(resp.responses))
        .map(_.asInstanceOf[Response.ResultSet])
        .toList

      closeFuture.isDefined must be(false)
      svc.status must be(Status.Open)

      val r1 :: r2 :: Nil = readers

      Await.result(Reader.readAllItems(r1.rows))
      svc.status must be(Status.Open)

      Await.result(Reader.readAllItems(r2.rows))
      Await.result(closeFuture)
      svc.status must be(Status.Closed)
    }

    "pass-through non-streaming responses" in {
      val svc = new MockService(1)
      val delayed = new DelayedReleaseService(svc)

      val fut = delayed(Request.Sync)

      Await.result(fut)
      Await.result(delayed.close())

      svc.status must be(Status.Closed)
    }
  }
}
