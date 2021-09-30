package com.twitter.finagle.postgresql

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Failure
import com.twitter.finagle.FailureFlags
import com.twitter.finagle.PostgreSql
import com.twitter.finagle.Status
import com.twitter.finagle.postgresql.FrontendMessage.DescriptionTarget
import com.twitter.finagle.postgresql.Response.QueryResponse
import com.twitter.finagle.postgresql.Response.SimpleQueryResponse
import com.twitter.finagle.postgresql.Types.Name
import com.twitter.finagle.postgresql.mock.MockPgTransport
import com.twitter.io.Reader
import com.twitter.io.ReaderDiscardedException
import com.twitter.util._
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ClientDispatcherSpec extends AnyWordSpec with Matchers with PropertiesSpec {
  import MockPgTransport._

  "ClientDispatcher" should {
    "send a statement timeout when set" in {
      val stackParams = PostgreSql.defaultParams + Params.StatementTimeout(1.second)
      val expects = Seq(
        expect(
          FrontendMessage
            .StartupMessage(user = "postgres", params = Map("statement_timeout" -> "1000")),
          BackendMessage.AuthenticationOk,
          BackendMessage.ReadyForQuery(BackendMessage.NoTx)
        ),
        expect(FrontendMessage.Sync, BackendMessage.ReadyForQuery(BackendMessage.NoTx))
      )

      val transport = new MockPgTransport(expects)
      val dispatcher = new ClientDispatcher(transport, stackParams)

      val fut = dispatcher.apply(Request.Sync)
      Await.result(fut)
    }

    "send a session defaults when set" in {
      val params = Map("a" -> "b", "c" -> "d")
      val stackParams = PostgreSql.defaultParams + Params.SessionDefaults(params)
      val expects = Seq(
        expect(
          FrontendMessage.StartupMessage(user = "postgres", params = params),
          BackendMessage.AuthenticationOk,
          BackendMessage.ReadyForQuery(BackendMessage.NoTx)),
        expect(FrontendMessage.Sync, BackendMessage.ReadyForQuery(BackendMessage.NoTx))
      )

      val transport = new MockPgTransport(expects)
      val dispatcher = new ClientDispatcher(transport, stackParams)

      val fut = dispatcher.apply(Request.Sync)
      Await.result(fut)
    }

    "send connection initialization sql when set" in {
      val stackParams = PostgreSql.defaultParams + Params.ConnectionInitializationCommands(
        Seq("set x = y", "set a = b"))
      val expects = Seq(
        expect(
          FrontendMessage.StartupMessage(user = "postgres"),
          BackendMessage.AuthenticationOk,
          BackendMessage.ReadyForQuery(BackendMessage.NoTx)),
        expect(
          FrontendMessage.Query("set x = y;\nset a = b"),
          BackendMessage.CommandComplete(BackendMessage.CommandTag.Other("SET")),
          BackendMessage.ReadyForQuery(BackendMessage.NoTx)
        ),
        expect(FrontendMessage.Sync, BackendMessage.ReadyForQuery(BackendMessage.NoTx))
      )

      val transport = new MockPgTransport(expects)
      val dispatcher = new ClientDispatcher(transport, stackParams)

      val fut = dispatcher.apply(Request.Sync)
      Await.result(fut)
    }

    "returns simple query result" in {
      forAll(arbTestResultSet.arbitrary, minSuccessful(1)) { arb =>
        val stackParams = PostgreSql.defaultParams
        val expects = Seq(
          expect(
            FrontendMessage.StartupMessage(user = "postgres"),
            BackendMessage.AuthenticationOk,
            BackendMessage.ReadyForQuery(BackendMessage.NoTx)
          ),
          expect(
            FrontendMessage.Query("QUERY 1"),
            arb.desc +: arb.rows :+
              BackendMessage.CommandComplete(BackendMessage.CommandTag.Other("QUERY")) :+
              BackendMessage.ReadyForQuery(BackendMessage.NoTx): _*
          ),
          expect(
            FrontendMessage.Query("QUERY 2"),
            BackendMessage.CommandComplete(BackendMessage.CommandTag.Other("QUERY 2")),
            BackendMessage.ReadyForQuery(BackendMessage.NoTx)
          )
        )

        val transport = new MockPgTransport(expects)
        val dispatcher = new ClientDispatcher(transport, stackParams)

        val fut = dispatcher.apply(Request.Query("QUERY 1")).flatMap { response =>
          foreachQueryResponse(response) {
            case r: Response.ResultSet =>
              assert(r.fields == arb.desc.rowFields)
              assert(Await.result(Reader.readAllItems(r.rows)) == getRows(arb.rows))
              Future.Done
            case _ => fail()
          }
        }

        Await.result(fut)
        Await.result(dispatcher.apply(Request.Query("QUERY 2")))
      }
    }

    "returns simple query result, and recovers transport when discarded" in {
      forAll(arbTestResultSet.arbitrary, minSuccessful(1)) { arb =>
        val stackParams = PostgreSql.defaultParams + Params.CancelGracePeriod(Duration.Top)
        val expects = Seq(
          expect(
            FrontendMessage.StartupMessage(user = "postgres"),
            BackendMessage.AuthenticationOk,
            BackendMessage.ReadyForQuery(BackendMessage.NoTx)
          ),
          expect(
            FrontendMessage.Query("QUERY 1"),
            arb.desc +: arb.rows :+
              BackendMessage.CommandComplete(BackendMessage.CommandTag.Other("QUERY 1")) :+
              BackendMessage.ReadyForQuery(BackendMessage.NoTx): _*
          ),
          expect(
            FrontendMessage.Query("QUERY 2"),
            BackendMessage.CommandComplete(BackendMessage.CommandTag.Other("QUERY 2")),
            BackendMessage.ReadyForQuery(BackendMessage.NoTx)
          )
        )

        val transport = new MockPgTransport(expects)
        val dispatcher = new ClientDispatcher(transport, stackParams)

        val fut = dispatcher.apply(Request.Query("QUERY 1")).flatMap { response =>
          foreachQueryResponse(response) {
            case r: Response.ResultSet =>
              assert(r.fields == arb.desc.rowFields)
              r.rows.discard()
              Future.Done
            case _ => fail()
          }
        }
        Await.result(fut)

        assert(transport.status == Status.Open)
        Await.result(dispatcher.apply(Request.Query("QUERY 2")))
      }
    }

    "execute returns query result" in {
      forAll(arbTestResultSet.arbitrary, minSuccessful(1)) { arb =>
        val stackParams = PostgreSql.defaultParams
        val expects = Seq(
          expect(
            FrontendMessage.StartupMessage(user = "postgres"),
            BackendMessage.AuthenticationOk,
            BackendMessage.ReadyForQuery(BackendMessage.NoTx)
          ),
          expect(
            Seq(
              FrontendMessage.Describe(Name.Named("QUERY 1"), DescriptionTarget.Portal),
              FrontendMessage.Execute(Name.Named("QUERY 1"), 100),
              FrontendMessage.Flush
            ),
            arb.desc +: arb.rows :+
              BackendMessage.CommandComplete(BackendMessage.CommandTag.Other("QUERY"))
          ),
          expect(FrontendMessage.Sync, BackendMessage.ReadyForQuery(BackendMessage.NoTx)),
          expect(
            FrontendMessage.Query("QUERY 2"),
            BackendMessage.CommandComplete(BackendMessage.CommandTag.Other("QUERY 2")),
            BackendMessage.ReadyForQuery(BackendMessage.NoTx)
          )
        )

        val transport = new MockPgTransport(expects)
        val dispatcher = new ClientDispatcher(transport, stackParams)

        val fut = dispatcher
          .apply(
            Request.ResumePortal(Name.Named("QUERY 1"), 100)
          ).flatMap {
            case r: Response.ResultSet =>
              assert(r.fields == arb.desc.rowFields)
              assert(Await.result(Reader.readAllItems(r.rows)) == getRows(arb.rows))
              Future.Done
            case _ => fail()
          }

        Await.result(fut)
        Await.result(dispatcher.apply(Request.Query("QUERY 2")))
      }
    }

    "execute returns query result, and recovers transport when interrupted" in {
      forAll(arbTestResultSet.arbitrary, minSuccessful(1)) { arb =>
        val mockTimer = new MockTimer
        val stackParams = PostgreSql.defaultParams +
          Params.CancelGracePeriod(1.second) + com.twitter.finagle.param.Timer(mockTimer)
        val expects = Seq(
          expect(
            FrontendMessage.StartupMessage(user = "postgres"),
            BackendMessage.AuthenticationOk,
            BackendMessage.ReadyForQuery(BackendMessage.NoTx)
          ),
          expect(
            Seq(
              FrontendMessage.Describe(Name.Named("QUERY 1"), DescriptionTarget.Portal),
              FrontendMessage.Execute(Name.Named("QUERY 1"), 100),
              FrontendMessage.Flush
            ),
            Seq(arb.desc)
          ),
          suspend("QUERY 1"),
          read(
            arb.rows :+
              BackendMessage.CommandComplete(BackendMessage.CommandTag.Other("QUERY")): _*
          ),
          expect(FrontendMessage.Sync, BackendMessage.ReadyForQuery(BackendMessage.NoTx)),
          expect(
            FrontendMessage.Query("QUERY 2"),
            BackendMessage.CommandComplete(BackendMessage.CommandTag.Other("QUERY 2")),
            BackendMessage.ReadyForQuery(BackendMessage.NoTx)
          )
        )

        val transport = new MockPgTransport(expects)
        val dispatcher = new ClientDispatcher(transport, stackParams)

        Time.withCurrentTimeFrozen { timeCtl =>
          val fut = dispatcher
            .apply(
              Request.ResumePortal(Name.Named("QUERY 1"), 100)
            ).flatMap {
              case r: Response.ResultSet =>
                assert(r.fields == arb.desc.rowFields)
                Reader.readAllItemsInterruptible(r.rows)
              case _ => fail()
            }

          timeCtl.advance(500.millis)
          mockTimer.tick()
          fut.raise(new FutureCancelledException)

          transport.resume("QUERY 1")

          intercept[ReaderDiscardedException] {
            Await.result(fut)
          }

          assert(transport.status == Status.Open)

          Await.result(dispatcher.apply(Request.Query("QUERY 2")))
        }
      }
    }

    "execute returns query result, and closes transport when timeout expires" in {
      forAll(arbTestResultSet.arbitrary, minSuccessful(1)) { arb =>
        val mockTimer = new MockTimer
        val stackParams = PostgreSql.defaultParams +
          Params.CancelGracePeriod(1.second) + com.twitter.finagle.param.Timer(mockTimer)
        val expects = Seq(
          expect(
            FrontendMessage.StartupMessage(user = "postgres"),
            BackendMessage.AuthenticationOk,
            BackendMessage.ReadyForQuery(BackendMessage.NoTx)
          ),
          expect(
            Seq(
              FrontendMessage.Describe(Name.Named("QUERY 1"), DescriptionTarget.Portal),
              FrontendMessage.Execute(Name.Named("QUERY 1"), 100),
              FrontendMessage.Flush
            ),
            arb.desc +: arb.rows,
          ),
          suspend("QUERY 1"),
        )

        val transport = new MockPgTransport(expects)
        val dispatcher = new ClientDispatcher(transport, stackParams)

        Time.withCurrentTimeFrozen { timeCtl =>
          val fut = dispatcher
            .apply(
              Request.ResumePortal(Name.Named("QUERY 1"), 100)
            ).flatMap {
              case r: Response.ResultSet =>
                assert(r.fields == arb.desc.rowFields)
                Reader.readAllItemsInterruptible(r.rows)
              case _ => fail()
            }

          fut.raise(new FutureCancelledException)
          assert(transport.status == Status.Open)

          timeCtl.advance(1.second)
          mockTimer.tick()

          intercept[ReaderDiscardedException] {
            Await.result(fut)
          }

          assert(transport.status == Status.Closed)
        }
      }
    }

    "execute returns query result, and closes transport when timeout is zero" in {
      forAll(arbTestResultSet.arbitrary, minSuccessful(1)) { arb =>
        val stackParams = PostgreSql.defaultParams
        val expects = Seq(
          expect(
            FrontendMessage.StartupMessage(user = "postgres"),
            BackendMessage.AuthenticationOk,
            BackendMessage.ReadyForQuery(BackendMessage.NoTx)
          ),
          expect(
            Seq(
              FrontendMessage.Describe(Name.Named("QUERY 1"), DescriptionTarget.Portal),
              FrontendMessage.Execute(Name.Named("QUERY 1"), 100),
              FrontendMessage.Flush
            ),
            arb.desc +: arb.rows,
          ),
          suspend("QUERY 1"),
        )

        val transport = new MockPgTransport(expects)
        val dispatcher = new ClientDispatcher(transport, stackParams)

        val fut = dispatcher
          .apply(
            Request.ResumePortal(Name.Named("QUERY 1"), 100)
          ).flatMap {
            case r: Response.ResultSet =>
              assert(r.fields == arb.desc.rowFields)
              Reader.readAllItemsInterruptible(r.rows)
            case _ => fail()
          }

        fut.raise(new FutureCancelledException)

        intercept[ReaderDiscardedException] {
          Await.result(fut)
        }

        assert(transport.status == Status.Closed)
      }
    }

    "when dispatch interrupted, closes transport" in {
      val stackParams = PostgreSql.defaultParams

      val expects = Seq(
        expect(
          FrontendMessage.StartupMessage(user = "postgres"),
          BackendMessage.AuthenticationOk,
          BackendMessage.ReadyForQuery(BackendMessage.NoTx)),
        write(
          FrontendMessage.Query("TEST"),
        ),
        suspend("TEST"),
      )

      val transport = new MockPgTransport(expects)
      val dispatcher = new ClientDispatcher(transport, stackParams)

      val fut = dispatcher.apply(Request.Query("TEST"))
      assert(!fut.isDefined)

      assert(transport.status == Status.Open)
      fut.raise(new FutureCancelledException)
      assert(transport.status == Status.Closed)

      intercept[FutureCancelledException] {
        Await.result(fut)
      }
    }

    "when dispatch interrupted, closes transport after CancelTimeout expires" in {
      val mockTimer = new MockTimer
      val stackParams = PostgreSql.defaultParams +
        Params.CancelGracePeriod(1.second) + com.twitter.finagle.param.Timer(mockTimer)

      val expects = Seq(
        expect(
          FrontendMessage.StartupMessage(user = "postgres"),
          BackendMessage.AuthenticationOk,
          BackendMessage.ReadyForQuery(BackendMessage.NoTx)),
        write(FrontendMessage.Query("TEST")),
        suspend("TEST"),
      )

      val transport = new MockPgTransport(expects)
      val dispatcher = new ClientDispatcher(transport, stackParams)

      Time.withCurrentTimeFrozen { timeCtl =>
        val fut = dispatcher.apply(Request.Query("TEST"))
        assert(!fut.isDefined)

        fut.raise(new FutureCancelledException)
        assert(transport.status == Status.Open)
        timeCtl.advance(1.second)
        mockTimer.tick()
        assert(transport.status == Status.Closed)

        intercept[FutureCancelledException] {
          Await.result(fut)
        }
      }
    }

    "when dispatch interrupted, doesn't close transport if query completed before CancelTimeout expires" in {
      val mockTimer = new MockTimer
      val stackParams = PostgreSql.defaultParams +
        Params.CancelGracePeriod(1.second) + com.twitter.finagle.param.Timer(mockTimer)

      val expects = Seq(
        expect(
          FrontendMessage.StartupMessage(user = "postgres"),
          BackendMessage.AuthenticationOk,
          BackendMessage.ReadyForQuery(BackendMessage.NoTx)),
        write(FrontendMessage.Query("TEST")),
        suspend("TEST"),
        read(
          BackendMessage.CommandComplete(BackendMessage.CommandTag.Other("TEST")),
          BackendMessage.ReadyForQuery(BackendMessage.NoTx)
        )
      )

      val transport = new MockPgTransport(expects)
      val dispatcher = new ClientDispatcher(transport, stackParams)

      Time.withCurrentTimeFrozen { timeCtl =>
        val fut = dispatcher.apply(Request.Query("TEST"))
        assert(!fut.isDefined)

        fut.raise(new FutureCancelledException)
        assert(transport.status == Status.Open)
        timeCtl.advance(500.millis)
        mockTimer.tick()
        assert(transport.status == Status.Open)

        transport.resume("TEST")

        Await.result(fut)
        assert(transport.status == Status.Open)
      }
    }

    "when dispatch interrupted, doesn't dispatch if already interrupted" in {
      val stackParams = PostgreSql.defaultParams

      val expects = Seq(
        expect(
          FrontendMessage.StartupMessage(user = "postgres"),
          BackendMessage.AuthenticationOk,
          BackendMessage.ReadyForQuery(BackendMessage.NoTx)),
        write(FrontendMessage.Query("TEST 1")),
        suspend("TEST 1"),
        read(
          BackendMessage.CommandComplete(BackendMessage.CommandTag.Other("TEST 1")),
          BackendMessage.ReadyForQuery(BackendMessage.NoTx)
        ),
      )

      val transport = new MockPgTransport(expects)
      val dispatcher = new ClientDispatcher(transport, stackParams)

      val fut1 = dispatcher.apply(Request.Query("TEST 1"))
      assert(!fut1.isDefined)

      val fut2 = dispatcher.apply(Request.Query("TEST 2"))
      assert(!fut2.isDefined)

      fut2.raise(new FutureCancelledException)
      assert(transport.status == Status.Open)

      transport.resume("TEST 1")

      Await.result(fut1)

      val fut2Failure = intercept[Failure] {
        Await.result(fut2)
      }
      assert(fut2Failure.isFlagged(FailureFlags.Interrupted))
      assert(transport.status == Status.Open)
    }
  }

  private def foreachQueryResponse(
    response: Response
  )(
    f: QueryResponse => Future[Unit]
  ): Future[Unit] = response match {
    case response: QueryResponse =>
      f(response)

    case response: SimpleQueryResponse =>
      var isDone = false
      Future.whileDo(!isDone) {
        response.responses.read().flatMap {
          case Some(r) => f(r)
          case None =>
            isDone = true
            Future.Done
        }
      }

    case _ =>
      Future.Done
  }

  private def getRows(msgs: Seq[BackendMessage.DataRow]): Seq[Response.Row] = {
    msgs.map(_.values)
  }
}
