package com.twitter.finagle.mysql.integration

import com.twitter.conversions.time._
import com.twitter.finagle.client.DefaultPool
import com.twitter.util.Await
import org.scalatest.FunSuite

class AbortedClientTest extends FunSuite with IntegrationClient {

  private def idleTime = 1.seconds

  override def configureClient(username: String, password: String, db: String) =
    super
      .configureClient(username, password, db)
      // Configure the connection pool such that connections aren't kept around long.
      .configured(
        DefaultPool.Param(
          // Don't keep any minimum of connections in the pool.
          low = 0,
          high = 100,
          bufferSize = 0,
          // Set idleTime to a short duration, so the connection pool will close old connections quickly.
          idleTime = idleTime,
          maxWaiters = 100
        )
      )

  for (c <- client) {
    test("MySql connections are closed cleanly, so MySql doesn't count them as aborted.") {
      val abortedClientQuery = "SHOW GLOBAL STATUS LIKE '%Aborted_clients%'"
      val initialAbortedValue: String = Await
        .result(c.select(abortedClientQuery) { row =>
          row.stringOrNull("Value")
        }, 5.seconds)
        .head

      val query = "SELECT '1' as ONE, '2' as TWO from information_schema.processlist;"
      // Run a query so the mysql client gets used
      Await.result(c.select(query) { row =>
        row("ONE").get
        row("TWO").get
      }, 5.seconds)

      // Wait a bit longer than the idleTime so the connection used above is removed from the pool.
      Thread.sleep((idleTime + 5.seconds).inMilliseconds)

      Await.result(c.select(abortedClientQuery) { row =>
        val abortedValue = row.stringOrNull("Value")
        assert(initialAbortedValue.toInt == abortedValue.toInt)
      }, 5.seconds)
    }
  }
}
