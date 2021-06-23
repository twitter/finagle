package com.twitter.finagle.mysql.integration

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.mysql._
import com.twitter.finagle.mysql.harness.EmbeddedSuite
import com.twitter.finagle.mysql.harness.config.{DatabaseConfig, InstanceConfig}
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.{IndividualRequestTimeoutException, Mysql, mysql}
import com.twitter.util.{Closable, Future}
import java.sql.Date
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import scala.collection.mutable.ArrayBuffer

case class SwimmingRecord(
  event: String,
  time: Float,
  name: String,
  nationality: String,
  date: Date) {
  override def toString: String = {
    def q(s: String) = "'" + s + "'"
    "(" + q(event) + "," + time + "," + q(name) + "," + q(nationality) + "," + q(
      date.toString) + ")"
  }
}

object SwimmingRecord {
  val schema = """CREATE TABLE IF NOT EXISTS `finagle-mysql-test` (
    `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
    `event` varchar(30) DEFAULT NULL,
    `time` float DEFAULT NULL,
    `name` varchar(40) DEFAULT NULL,
    `nationality` varchar(20) DEFAULT NULL,
    `date` date DEFAULT NULL,
    PRIMARY KEY (`id`)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""

  val allRecords: List[SwimmingRecord] = List[SwimmingRecord](
    SwimmingRecord("50 m freestyle", 20.91f, "Cesar Cielo", "Brazil", Date.valueOf("2009-12-18")),
    SwimmingRecord("100 m freestyle", 46.91f, "Cesar Cielo", "Brazil", Date.valueOf("2009-08-02")),
    SwimmingRecord(
      "50 m backstroke",
      24.04f,
      "Liam Tancock",
      "Great Britain",
      Date.valueOf("2009-08-02")
    ),
    SwimmingRecord(
      "100 m backstroke",
      51.94f,
      "Aaron Peirsol",
      "United States",
      Date.valueOf("2009-07-08")
    ),
    SwimmingRecord("50 m butterfly", 22.43f, "Rafael Munoz", "Spain", Date.valueOf("2009-05-05")),
    SwimmingRecord(
      "100 m butterfly",
      49.82f,
      "Michael Phelps",
      "United States",
      Date.valueOf("2009-07-29")
    ),
    // this record is used to check how empty strings are handled.
    SwimmingRecord(
      "",
      100.0f,
      "",
      "",
      Date.valueOf("2009-08-02")
    )
  )
}

class ClientTest extends EmbeddedSuite with BeforeAndAfterAll with Eventually {
  import SwimmingRecord._

  val maxConcurrentPreparedStatements: Int = 10

  val instanceConfig: InstanceConfig = defaultInstanceConfig
  val databaseConfig: DatabaseConfig =
    DatabaseConfig(databaseName = "b_database", users = Seq.empty, setupQueries = Seq(schema))

  test("failed auth") { fixture =>
    try {
      await(Mysql.newRichClient(fixture.instance.dest).ping())
      fail("Expected an error when using an unauthenticated client")
    } catch {
      // Expected Access Denied Error Code
      case se: ServerError => assert(se.code == 1045)
    }
  }

  test("ping") { fixture =>
    val client = fixture.newRichClient()
    await(client.ping())
  }

  private val createTableSql =
    """CREATE TABLE IF NOT EXISTS table_create_test (id int(5))"""

  test("query: create a table") { fixture =>
    val client = fixture.newRichClient()
    val createResult = await(client.query(createTableSql))
    assert(createResult.isInstanceOf[OK])
  }

  test("modify: create a table") { fixture =>
    val client = fixture.newRichClient()
    await(client.modify(createTableSql))
  }

  test("query: insert values") { fixture =>
    val insertSql =
      """INSERT INTO `finagle-mysql-test` (`event`, `time`, `name`, `nationality`, `date`)
        |VALUES %s;""".stripMargin
        .format(allRecords.mkString(", "))
    val client = fixture.newRichClient()
    val insertResult = await(client.query(insertSql))
    val ok = insertResult.asInstanceOf[OK]
    assert(ok.insertId == 1)
  }

  test("modify: insert values") { fixture =>
    // other tests are dependent on the data setup, so we are mindful
    // to not modify any rows.
    val insertSql =
      """
        |INSERT INTO `finagle-mysql-test` (event, time, name, nationality, date)
        |SELECT 'event', 1.0, 'name', 'nationality', 'date'
        |WHERE 1 = 0
      """.stripMargin
    val client = fixture.newRichClient()
    val insertResult = await(client.modify(insertSql))
    assert(insertResult.affectedRows == 0)
  }

  test("read: select values") { fixture =>
    val client = fixture.newRichClient()
    val resultSet =
      await(client.read("SELECT * FROM `finagle-mysql-test`"))
    assert(resultSet.rows.size == allRecords.size)
  }

  test("select: select values") { fixture =>
    val client = fixture.newRichClient()
    val selectResult =
      await(client.select("SELECT * FROM `finagle-mysql-test`") { row =>
        val event = row.stringOrNull("event")
        val time = row.floatOrZero("time")
        val name = row.stringOrNull("name")
        val nation = row.stringOrNull("nationality")
        val date = row.javaSqlDateOrNull("date")
        SwimmingRecord(event, time, name, nation, date)
      })

    var i = 0
    for (res <- selectResult) {
      assert(allRecords(i) == res)
      i += 1
    }
  }

  test("can execute more prepared statements than allowed in cache") { fixture =>
    val client = fixture
      .newClient()
      .withMaxConcurrentPrepareStatements(maxConcurrentPreparedStatements)
      .newRichClient(fixture.instance.dest)
    val queryStrings = (0 to (maxConcurrentPreparedStatements * 2)).map { i => s"SELECT $i" }
    val queryResults = Future.collect(queryStrings.map { query =>
      client.prepare(query)().map(_ => "ok")
    })
    val results = await(queryResults)
    results.map { result => assert(result == "ok") }
  }

  test("prepared statement") { fixture =>
    val prepareQuery =
      "SELECT COUNT(*) AS 'numRecords' FROM `finagle-mysql-test` WHERE `name` LIKE ?"
    val client = fixture.newRichClient()
    val ps = client.prepare(prepareQuery)
    for (i <- 0 to 10) {
      val randomIdx = math.floor(math.random * (allRecords.size - 1)).toInt
      val recordName = allRecords(randomIdx).name
      val expectedRes = LongValue(allRecords.filter(_.name == recordName).size)
      val res = ps.select(recordName)(identity)
      val row = await(res)(0)
      assert(row("numRecords").get == expectedRes)
    }
  }

  test("cursored statement") { fixture =>
    val query = "select * from `finagle-mysql-test` where `event` = ?"
    val client = fixture.newRichClient()
    val cursoredStatement = client.cursor(query)
    val cursorResult = await(cursoredStatement(1, "50 m freestyle")(r => r))
    val rows = await(cursorResult.stream.toSeq())

    assert(rows.size == 1)
    assert(rows(0)("event").get == StringValue("50 m freestyle"))
  }

  test("query with invalid sql includes sql in exception message") { fixture =>
    // this has a mismatched number of columns.
    val invalidSql =
      """INSERT INTO `finagle-mysql-test` (
        |  `event`, `time`, `name`, `nationality`, `date`
        |) VALUES (1)""".stripMargin

    val client = fixture.newRichClient()

    val err = intercept[ServerError] {
      await(client.query(invalidSql))
    }
    assert(err.getMessage.contains(invalidSql))
  }

  test("select with invalid sql includes sql in exception message") { fixture =>
    // this has a mismatched number of columns.
    val invalidSql =
      """
        |SELECT 1
        |FROM `finagle-mysql-test`
        |WHERE `event` IN (SELECT ?, ?)
      """.stripMargin

    val client = fixture.newRichClient()

    val err = intercept[ServerError] {
      await(client.select(invalidSql)(identity))
    }
    assert(err.getMessage.contains(invalidSql))
  }

  test("prepare with invalid sql includes sql in exception message") { fixture =>
    val invalidSql =
      """INSERT INTO `finagle-mysql-test` (
        |  `event`, `time`, `name`, `nationality`, `date`
        |) VALUES (?)""".stripMargin
    val client = fixture.newRichClient()
    val prepared = client.prepare(invalidSql)
    val err = intercept[ServerError] {
      await(prepared(1))
    }
    assert(err.getMessage.contains(invalidSql))
  }

  test("cursor with invalid sql includes sql in exception message") { fixture =>
    val invalidSql =
      """
        |SELECT 1
        |FROM `finagle-mysql-test`
        |WHERE `event` IN (SELECT ?, ?)
      """.stripMargin
    val client = fixture.newRichClient()
    val statement = client.cursor(invalidSql)
    val cursor = await(statement(1, "X", "Y")(identity))
    val err = intercept[ServerError] {
      await(cursor.stream.toSeq())
    }
    assert(err.getMessage.contains(invalidSql))
  }

  test("prepare can read records with empty strings") { fixture =>
    val sql =
      """
        |SELECT *
        |FROM `finagle-mysql-test`
        |WHERE `name` = ?
      """.stripMargin
    val client = fixture.newRichClient()
    val prepared = client.prepare(sql)
    val res = prepared.select("")(identity)
    val rows = await(res)
    assert(rows.size == 1)
    assert(rows.head("time").get == FloatValue(100))
  }

  test("CursorResult does not store head of stream") { fixture =>
    val query = "select * from `finagle-mysql-test`"
    val client = fixture.newRichClient()
    val cursoredStatement = client.cursor(query)
    val cursorResult = await(cursoredStatement(1)(r => r))
    val first = cursorResult.stream.take(1)
    val second = cursorResult.stream.take(1)
    assert(first != second)
    cursorResult.close()
  }

  // NOTE: This relies on the timeout being shorter than the time it takes for
  // the benchmark to run (1.34 sec). If the machine finishes the query before
  // the timeout then it's possible the test will fail.
  //
  // We may wish to mark this test flaky if we observe that it fails.
  test("client connection closed on interrupt") { fixture =>
    val stats: InMemoryStatsReceiver = new InMemoryStatsReceiver()

    val client = fixture.newRichClient()
    val timeoutClient =
      fixture
        .newClient()
        .withMaxConcurrentPrepareStatements(maxConcurrentPreparedStatements)
        .withLabel("timeoutClient")
        .withStatsReceiver(stats)
        .withRequestTimeout(100.milliseconds)
        .newRichClient(fixture.instance.dest)

    def poolSize: Int = {
      stats.gauges.get(Seq("timeoutClient", "pool_size")) match {
        case Some(f) => f().toInt
        case None => -1
      }
    }

    val processesQuery: String = "select * from information_schema.processlist"

    // Running time on an early 2015 MacBook Pro.
    // mysql> select benchmark(100000000, 1+1);
    // <snip>
    // 1 row in set (1.34 sec)
    val expensiveQuery: String = "select benchmark(100000000, 1+1)"
    val res: Future[Result] = timeoutClient.query(expensiveQuery)

    intercept[IndividualRequestTimeoutException] {
      await(res)
    }

    // Query timed out but it's still running on the server.
    def processes: String =
      await(client.query(processesQuery)).asInstanceOf[ResultSet].rows.toString
    assert(processes.contains(expensiveQuery))

    // Client connection closed.
    eventually {
      assert(poolSize == 0)
    }
  }

  test("stored procedure returns result set") { fixture =>
    val createProcedure =
      """
        |create procedure getSwimmerByEvent(IN eventName varchar(30))
        |begin
        |select *
        |from `finagle-mysql-test`
        |where `event` = convert(eventName using utf8) collate utf8_general_ci;
        |end
      """.stripMargin

    val executeProcedure =
      """
        |call getSwimmerByEvent('50 m freestyle')
      """.stripMargin

    val dropProcedure =
      """
        |drop procedure if exists getSwimmerByEvent
      """.stripMargin

    val client = fixture.newRichClient()
    // Drop the procedure if it was left over from a previously
    // failed run.
    await(client.query(dropProcedure))
    await(client.query(createProcedure))

    val result = await(
      client.select(executeProcedure) { row => row.stringOrNull("name") }
    )

    assert(result == List("Cesar Cielo"))
    await(client.query(dropProcedure))
  }

  test("mysql server error during handshake is reported with error code") { fixture =>
    // The default maximum number of connections is 150, so we open 151.
    val clients = new ArrayBuffer[mysql.Client]()
    try {
      val err = intercept[Exception] {
        for (_ <- 0 to 150) {
          val newClient = fixture
            .newClient()
            .withMaxConcurrentPrepareStatements(maxConcurrentPreparedStatements)
            .newRichClient(fixture.instance.dest)
          clients += newClient
          await(newClient.ping)
        }
      }
      val rootErrorMsg = "Exception in MySQL handshake, error code 1040"
      val nonRootErrorMsg = "Too many connections"
      assert(err.getMessage.contains(rootErrorMsg) || err.getMessage.contains(nonRootErrorMsg))
    } finally {
      Closable.all(clients: _*).close()
    }
  }

}
