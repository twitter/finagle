package com.twitter.finagle.mysql.integration

import com.twitter.finagle.mysql.harness.EmbeddedSuite
import com.twitter.finagle.mysql.harness.config.{DatabaseConfig, InstanceConfig}
import com.twitter.finagle.mysql.{IsolationLevel, LongValue, ServerError, StdClient}
import com.twitter.finagle.stats.InMemoryStatsReceiver
import org.scalatest.concurrent.{Eventually, IntegrationPatience}

class TransactionTest extends EmbeddedSuite with Eventually with IntegrationPatience {

  val instanceConfig: InstanceConfig = defaultInstanceConfig
  val databaseConfig: DatabaseConfig = defaultDatabaseConfig

  test("Simple transaction") { fixture =>
    val client = fixture.newRichClient()
    assertResult(Seq(Seq(LongValue(1)))) {
      await(client.transaction { client => client.select("SELECT 1") { row => row.values } })
    }
  }

  test("Simple transaction with isolation level") { fixture =>
    val client = fixture.newRichClient()
    val isolationLevels = Seq(
      IsolationLevel.ReadCommitted,
      IsolationLevel.ReadUncommitted,
      IsolationLevel.RepeatableRead,
      IsolationLevel.Serializable
    )

    for (iso <- isolationLevels) {
      assertResult(Seq(Seq(LongValue(1)))) {
        await(client.transactionWithIsolation(iso) { client =>
          client.select("SELECT 1") { row => row.values }
        })
      }
    }
  }

  test("transaction fails during rollback") { fixture =>
    val stats = new InMemoryStatsReceiver()
    val finagleClient = fixture
      .newClient()
      .withLabel("mysqlClient")
      .withStatsReceiver(stats)
      .newClient(fixture.instance.dest)

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
