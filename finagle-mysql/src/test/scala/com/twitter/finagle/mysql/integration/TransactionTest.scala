package com.twitter.finagle.mysql.integration

import com.twitter.conversions.time._
import com.twitter.finagle.mysql.{IsolationLevel, LongValue, ServerError, StdClient}
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.{Await, Future}
import org.scalatest.FunSuite
import org.scalatest.concurrent.{Eventually, IntegrationPatience}

class TransactionTest extends FunSuite
  with IntegrationClient
  with Eventually
  with IntegrationPatience {

  private def await[T](f: Future[T]): T = Await.result(f, 5.seconds)

  test("Simple transaction") {
    for (c <- client) {
      assertResult(Seq(Seq(LongValue(1)))) {
        await(c.transaction { client =>
          client.select("SELECT 1") { row =>
            row.values
          }
        })
      }
    }
  }

  test("Simple transaction with isolation level") {
    for (c <- client) {
      val isolationLevels = Seq(
        IsolationLevel.ReadCommitted,
        IsolationLevel.ReadUncommitted,
        IsolationLevel.RepeatableRead,
        IsolationLevel.Serializable
      )

      for (iso <- isolationLevels) {
        assertResult(Seq(Seq(LongValue(1)))) {
          await(c.transactionWithIsolation(iso) { client =>
            client.select("SELECT 1") { row =>
              row.values
            }
          })
        }
      }
    }
  }

  test("transaction fails during rollback") {
    val stats = new InMemoryStatsReceiver()
    val finagleClient = configureClient()
      .withLabel("mysqlClient")
      .withStatsReceiver(stats)
      .newClient(dest)

    def poolSize: Int = {
      stats.gauges.get(Seq("mysqlClient", "pool_size")) match {
        case Some(f) => f().toInt
        case None => -1
      }
    }

    val client = new StdClient(finagleClient, false, stats, "BLOWUP_NOT_ROLLBACK")

    def select1(): Unit = {
      val ones = await(client.select("SELECT 1") { row =>
        val LongValue(v) = row.values.head
        v
      })
      assert(Seq(1) == ones)
    }

    select1()

    val res = client.transaction { c =>
      c.query("select 1").flatMap { _ =>
        // verify our baseline connection metric
        eventually { assert(poolSize == 1) }
        // purposefully trigger a rollback
        c.query("thisWillFail")
      }
    }
    intercept[ServerError] {
      await(res)
    }

    // verify the close happens.
    eventually {
      assert(poolSize == 0)
    }

    select1()
  }
}
