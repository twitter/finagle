package com.twitter.finagle.mysql.integration

import com.twitter.finagle.mysql.harness.EmbeddedSimpleSuite
import com.twitter.finagle.mysql.harness.config.{DatabaseConfig, InstanceConfig}
import com.twitter.finagle.mysql.{Client, OK, ServerError}
import com.twitter.util.Future
import java.sql.SQLException
import org.scalatest.BeforeAndAfter

private object PreparedStatementTest {
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

class PreparedStatementTest extends EmbeddedSimpleSuite with BeforeAndAfter {
  import PreparedStatementTest._

  def instanceConfig: InstanceConfig = defaultInstanceConfig
  def databaseConfig: DatabaseConfig = defaultDatabaseConfig

  private[this] val c: Client = fixture match {
    case Some(f) => f.newRichClient()
    case None => null
  }

  before {
    if (c != null)
      await(c.modify(createTable))
  }

  after {
    if (c != null)
      await(c.modify("""DROP TEMPORARY TABLE IF EXISTS prepared_stmt"""))
  }

  /** Returns the primary key for the inserted row */
  private[this] def insertBigDecimal(bd: Option[BigDecimal]): Long = {
    val preparedInsert = c.prepare(insertBigDecimalSql)
    val inserted: Future[OK] = preparedInsert.modify(bd)
    val result = inserted.flatMap { ok =>
      if (ok.affectedRows == 1)
        Future.value(ok.insertId)
      else
        Future.exception(new SQLException(s"did not insert exactly 1 row: $ok"))
    }
    await(result)
  }

  private[this] def readBigDecimal(id: Long): BigDecimal = {
    val selectSql = "SELECT big_decimal FROM prepared_stmt WHERE id = ?"
    val stmt = c.prepare(selectSql)
    val resultSet = await(stmt.read(id))
    assert(resultSet.rows.size == 1)
    resultSet.rows.head.bigDecimalOrNull("big_decimal")
  }

  private[this] def selectBigDecimal(id: Long): BigDecimal = {
    val selectSql = "SELECT big_decimal FROM prepared_stmt WHERE id = ?"
    val stmt = c.prepare(selectSql)
    val bds = await(stmt.select(id) { row => row.bigDecimalOrNull("big_decimal") })
    assert(bds.size == 1)
    assert(bds.head == readBigDecimal(id))

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
    // Depending on how the server is configured, this test may fail.
    // With the following configurations (which happen to be MySQL's defaults) this test will pass:
    // +-----------------------------------------------------------------------------------------------------------------------+
    // | @@sql_mode                                                                                                            |
    // +-----------------------------------------------------------------------------------------------------------------------+
    // | ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION |
    // +-----------------------------------------------------------------------------------------------------------------------+
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
