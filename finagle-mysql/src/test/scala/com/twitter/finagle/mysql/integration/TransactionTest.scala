package com.twitter.finagle.mysql.integration

import com.twitter.finagle.mysql.{IsolationLevel, LongValue}
import com.twitter.util.{Await, Duration}
import org.scalatest.FunSuite

class TransactionTest extends FunSuite with IntegrationClient {
  val defaultTimeout = Duration.fromSeconds(1)

  test("Simple transaction") {
    for (c <- client) {
      assertResult(Seq(Seq(LongValue(1)))) {
        Await.result(c.transaction { client =>
          client.select("SELECT 1") { row =>
            row.values
          }
        }, defaultTimeout)
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
          Await.result(c.transactionWithIsolation(iso) { client =>
            client.select("SELECT 1") { row =>
              row.values
            }
          }, defaultTimeout)
        }
      }
    }
  }
}
