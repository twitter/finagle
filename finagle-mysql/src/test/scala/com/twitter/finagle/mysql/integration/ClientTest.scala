package com.twitter.finagle.mysql.integration

import com.twitter.conversions.time._
import com.twitter.finagle.Mysql
import com.twitter.finagle.mysql._
import com.twitter.util.{Await, Future}
import java.sql.Date
import org.scalatest.{BeforeAndAfterAll, FunSuite}

case class SwimmingRecord(
  event: String,
  time: Float,
  name: String,
  nationality: String,
  date: Date
) {
  override def toString: String = {
    def q(s: String) = "'" + s + "'"
    "(" + q(event) + "," + time + "," + q(name) + "," + q(nationality) + "," + q(date.toString) + ")"
  }
}

object SwimmingRecord {
  val schema = """CREATE TEMPORARY TABLE IF NOT EXISTS `finagle-mysql-test` (
    `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
    `event` varchar(30) DEFAULT NULL,
    `time` float DEFAULT NULL,
    `name` varchar(40) DEFAULT NULL,
    `nationality` varchar(20) DEFAULT NULL,
    `date` date DEFAULT NULL,
    PRIMARY KEY (`id`)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""

  val allRecords: List[SwimmingRecord] = List[SwimmingRecord](
    SwimmingRecord("50 m freestyle", 20.91F, "Cesar Cielo", "Brazil", Date.valueOf("2009-12-18")),
    SwimmingRecord("100 m freestyle", 46.91F, "Cesar Cielo", "Brazil", Date.valueOf("2009-08-02")),
    SwimmingRecord(
      "50 m backstroke",
      24.04F,
      "Liam Tancock",
      "Great Britain",
      Date.valueOf("2009-08-02")
    ),
    SwimmingRecord(
      "100 m backstroke",
      51.94F,
      "Aaron Peirsol",
      "United States",
      Date.valueOf("2009-07-08")
    ),
    SwimmingRecord("50 m butterfly", 22.43F, "Rafael Munoz", "Spain", Date.valueOf("2009-05-05")),
    SwimmingRecord(
      "100 m butterfly",
      49.82F,
      "Michael Phelps",
      "United States",
      Date.valueOf("2009-07-29")
    ),
    // this record is used to check how empty strings are handled.
    SwimmingRecord(
      "",
      100.0F,
      "",
      "",
      Date.valueOf("2009-08-02")
    )
  )
}

class ClientTest extends FunSuite
  with IntegrationClient
  with BeforeAndAfterAll {
  import SwimmingRecord._

  private[this] def await[T](f: Future[T]): T =
    Await.result(f, 5.seconds)

  private val c: Client with Transactions = client.orNull

  override def beforeAll(): Unit = {
    if (c != null) {
      await(c.query(schema)) match {
        case _: OK => // ok, table created. good.
        case x => fail("Create table was not ok: " + x)
      }
    }
  }

  override def afterAll(): Unit = {
    if (c != null) {
      c.close()
    }
  }

  test("failed auth") {
    try {
      await(Mysql.newRichClient("localhost:3306").ping())
      fail("Expected an error when using an unauthenticated client")
    } catch {
      // Expected Access Denied Error Code
      case se: ServerError => assert(se.code == 1045)
    }
  }

  test("ping") {
    await(c.ping())
  }

  private val createTableSql =
    """CREATE TEMPORARY TABLE IF NOT EXISTS table_create_test (id int(5))"""

  test("query: create a table") {
    val createResult = await(c.query(createTableSql))
    assert(createResult.isInstanceOf[OK])
  }

  test("modify: create a table") {
    await(c.modify(createTableSql))
  }

  test("query: insert values") {
    val insertSql =
       """INSERT INTO `finagle-mysql-test` (`event`, `time`, `name`, `nationality`, `date`)
       VALUES %s;""".format(allRecords.mkString(", "))
    val insertResult = await(c.query(insertSql))
    val ok = insertResult.asInstanceOf[OK]
    assert(ok.insertId == 1)
  }

  test("modify: insert values") {
    // other tests are dependent on the data setup, so we are mindful
    // to not modify any rows.
    val insertSql =
      """
        |INSERT INTO `finagle-mysql-test` (event, time, name, nationality, date)
        |SELECT 'event', 1.0, 'name', 'nationality', 'date'
        |WHERE 1 = 0
      """.stripMargin
    val insertResult = await(c.modify(insertSql))
    assert(insertResult.affectedRows == 0)
  }

  test("read: select values") {
    val resultSet = await(c.read("SELECT * FROM `finagle-mysql-test`"))
    assert(resultSet.rows.size == allRecords.size)
  }

  test("select: select values") {
    val selectResult = await(c.select("SELECT * FROM `finagle-mysql-test`") { row =>
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

  test("prepared statement") {
    val prepareQuery =
      "SELECT COUNT(*) AS 'numRecords' FROM `finagle-mysql-test` WHERE `name` LIKE ?"
    val ps = c.prepare(prepareQuery)
    for (i <- 0 to 10) {
      val randomIdx = math.floor(math.random * (allRecords.size - 1)).toInt
      val recordName = allRecords(randomIdx).name
      val expectedRes = LongValue(allRecords.filter(_.name == recordName).size)
      val res = ps.select(recordName)(identity)
      val row = await(res)(0)
      assert(row("numRecords").get == expectedRes)
    }
  }

  test("cursored statement") {
    val query = "select * from `finagle-mysql-test` where `event` = ?"
    val cursoredStatement = c.cursor(query)
    val cursorResult = await(cursoredStatement(1, "50 m freestyle")(r => r))
    val rows = await(cursorResult.stream.toSeq())

    assert(rows.size == 1)
    assert(rows(0)("event").get == StringValue("50 m freestyle"))
  }

  test("query with invalid sql includes sql in exception message") {
    // this has a mismatched number of columns.
    val invalidSql =
      """INSERT INTO `finagle-mysql-test` (
        |  `event`, `time`, `name`, `nationality`, `date`
        |) VALUES (1)""".stripMargin

    val err = intercept[ServerError] {
      await(c.query(invalidSql))
    }
    assert(err.getMessage.contains(invalidSql))
  }

  test("select with invalid sql includes sql in exception message") {
    // this has a mismatched number of columns.
    val invalidSql =
      """
        |SELECT 1
        |FROM `finagle-mysql-test`
        |WHERE `event` IN (SELECT ?, ?)
      """.stripMargin

    val err = intercept[ServerError] {
      await(c.select(invalidSql)(identity))
    }
    assert(err.getMessage.contains(invalidSql))
  }

  test("prepare with invalid sql includes sql in exception message") {
    val invalidSql =
      """INSERT INTO `finagle-mysql-test` (
        |  `event`, `time`, `name`, `nationality`, `date`
        |) VALUES (?)""".stripMargin
    val prepared = c.prepare(invalidSql)
    val err = intercept[ServerError] {
      await(prepared(1))
    }
    assert(err.getMessage.contains(invalidSql))
  }

  test("cursor with invalid sql includes sql in exception message") {
    val invalidSql =
      """
        |SELECT 1
        |FROM `finagle-mysql-test`
        |WHERE `event` IN (SELECT ?, ?)
      """.stripMargin
    val statement = c.cursor(invalidSql)
    val cursor = await(statement(1, "X", "Y")(identity))
    val err = intercept[ServerError] {
      await(cursor.stream.toSeq())
    }
    assert(err.getMessage.contains(invalidSql))
  }

  test("PreparedStatement reading empty strings") {
    val sql =
      """
        |INSERT INTO
      """.stripMargin
  }

  // NOTE: this test case seems to do something bad to the client
  // and we leave it as the last test case and investigate separately.
  test("CursorResult does not store head of stream") {
    val query = "select * from `finagle-mysql-test`"
    val cursoredStatement = c.cursor(query)
    val cursorResult = await(cursoredStatement(1)(r => r))
    val first = cursorResult.stream.take(1)
    val second = cursorResult.stream.take(1)
    assert(first != second)
  }

}
