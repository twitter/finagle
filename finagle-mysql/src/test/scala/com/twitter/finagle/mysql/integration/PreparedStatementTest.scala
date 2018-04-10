package com.twitter.finagle.mysql.integration

import com.twitter.conversions.time._
import com.twitter.finagle.mysql.{Client, OK, Result, ServerError}
import com.twitter.util.{Await, Future}
import java.sql.SQLException
import org.scalatest.{BeforeAndAfter, FunSuite}

private object PreparedStatementTest {

  private def await[T](f: Future[T]): T =
    Await.result(f, 5.seconds)

  private val createTable =
    """
      |CREATE TEMPORARY TABLE IF NOT EXISTS prepared_stmt (
      |  id INT(5) NOT NULL AUTO_INCREMENT,
      |  big_decimal DECIMAL(5, 2) DEFAULT NULL,
      |  PRIMARY KEY (id)
      |)
    """.stripMargin

  private val insertBigDecimalSql =
    """
      |INSERT INTO prepared_stmt (
      |  big_decimal
      |) VALUES (
      |  ?
      |)
    """.stripMargin

}

class PreparedStatementTest extends FunSuite
  with IntegrationClient
  with BeforeAndAfter {
  import PreparedStatementTest._

  private[this] val c: Client = client.orNull

  before {
    await(c.query(createTable)) match {
      case _: OK => // ok, table created. good.
      case x => fail("Create table was not ok: " + x)
    }
  }

  /** Returns the primary key for the inserted row */
  private[this] def insertBigDecimal(bd: Option[BigDecimal]): Long = {
    val preparedInsert = c.prepare(insertBigDecimalSql)
    val inserted: Future[Result] = preparedInsert(bd)
    val result = inserted.flatMap {
      case ok: OK =>
        if (ok.affectedRows == 1)
          Future.value(ok.insertId)
        else
          Future.exception(new SQLException(s"did not insert exactly 1 row: $ok"))
      case notOk =>
        Future.exception(new SQLException(s"insert into failed: $notOk"))
    }
    await(result)
  }

  private[this] def selectBigDecimal(id: Long): BigDecimal = {
    val selectSql = s"SELECT big_decimal FROM prepared_stmt WHERE id = $id"
    val bds = await(c.select(selectSql) { row =>
      row.bigDecimalOrNull("big_decimal")
    })
    assert(bds.size == 1)
    bds.head
  }

  private[this] def testBigDecimal(bd: BigDecimal): Unit = {
    val id = insertBigDecimal(Option(bd))
    assert(bd == selectBigDecimal(id))
  }

  test("insert BigDecimal") {
    testBigDecimal(BigDecimal(100.05))
  }

  test("insert null BigDecimal") {
    testBigDecimal(null)
  }

  test("insert with Some") {
    val bd = BigDecimal(100.1)
    val id = insertBigDecimal(Some(bd))
    assert(bd == selectBigDecimal(id))
  }

  test("insert with None") {
    val id = insertBigDecimal(None)
    assert(null == selectBigDecimal(id))
  }

  test("insert BigDecimal with too much precision") {
    intercept[ServerError] {
      // this number has more total digits than allowed, 5
      insertBigDecimal(Some(BigDecimal("100000")))
    }
  }

  test("insert BigDecimal with too much scale") {
    // this number has more digits after the decimal point (3) than allowed (2)
    val bd = BigDecimal("100.888")
    val id = insertBigDecimal(Some(bd))
    assert(BigDecimal(100.89) == selectBigDecimal(id))
  }

}
