package com.twitter.finagle.mysql.unit

import com.twitter.finagle.exp.Mysql
import com.twitter.finagle.exp.mysql._
import com.twitter.finagle.mysql.util.{SqlExecutor, EmbeddableMysql}
import com.twitter.util.Await
import java.sql.Date
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, BeforeAndAfterAll, BeforeAndAfterEach}

case class SwimmingRecord(
    event: String,
    time: Float,
    name: String,
    nationality: String,
    date: Date
    ) {
  override def toString = {
    def q(s: String) = "'" + s + "'"
    "(" + q(event) + "," + time + "," + q(name) + "," + q(nationality) + "," + q(date.toString) + ")"
  }
}

@RunWith(classOf[JUnitRunner])
class ClientTest extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

  private val db: EmbeddableMysql = new EmbeddableMysql(user = "testuser", password = "password",
    db = "test")

  private var client: Client = _

  override def beforeAll() {
    db.start()
  }

  override def afterAll() {
    db.shutdown()
  }

  override def beforeEach() {
    client = db.createClient()
    Await.result(SqlExecutor.executeSql(client, Seq(
      getClass.getResource("/drop-schema.sql"),
      getClass.getResource("/create-schema.sql"),
      getClass.getResource("/data.sql"))))
  }

  test("failed auth") {
    try {
      Await.result(Mysql.newRichClient("localhost:%d".format(db.port)).ping)
      fail("Expected an error when using an unauthenticated client")
    } catch {
      // Expected Access Denied Error Code
      case ServerError(code, _, _) => assert(code == 1045)
      case _ => fail()
    }
  }

  test("ping") {
    val pingResult = Await.result(client.ping)
    assert(pingResult.isInstanceOf[OK])
  }

  test("query: create a table") {

    val schema = """CREATE TEMPORARY TABLE IF NOT EXISTS `finagle-mysql-test` (
    `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
    PRIMARY KEY (`id`)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""

    val createResult = Await.result(client.query(schema))
    assert(createResult.isInstanceOf[OK])
  }

  test("query: insert values") {
    val sql = (
        "INSERT INTO swimming_record (event, time, name, nationality, date) " +
        "VALUES (%s, %f, %s, %s, %s)")
        .format("'1 m freestyle'", 100.3, "'Slow Guy'", "'USA'", "'2014-01-01'")

    val insertResult = Await.result(client.query(sql))
    val OK(_, insertid, _, _, _) = insertResult.asInstanceOf[OK]
    assert(insertResult.isInstanceOf[OK])
    assert(insertid > 0)
  }

  test("query: select values") {

    val slowGuyRec = SwimmingRecord("1 m freestyle", 100.3F, "Slow Guy", "USA",
      Date.valueOf("2014-01-01"))
    Await.result(client.query(
      "INSERT INTO swimming_record (event, time, name, nationality, date) " +
      "VALUES ('%s', %f, '%s', '%s', '%s')"
      .format(slowGuyRec.event, slowGuyRec.time, slowGuyRec.name, slowGuyRec.nationality,
        slowGuyRec.date)))

    val fastGuyRec = SwimmingRecord("1 m freestyle", 0.9F, "Fast Guy", "USA",
      Date.valueOf("2014-01-01"))
    Await.result(client.query(
      "INSERT INTO swimming_record (event, time, name, nationality, date) " +
      "VALUES ('%s', %f, '%s', '%s', '%s')".format(
      fastGuyRec.event, fastGuyRec.time, fastGuyRec.name, fastGuyRec.nationality, fastGuyRec.date)))

    val selectResults = Await.result(client.select(
      "SELECT * FROM swimming_record ORDER BY id DESC LIMIT 2") { row =>
        val StringValue(event) = row("event").get
        val FloatValue(time) = row("time").get
        val StringValue(name) = row("name").get
        val StringValue(nation) = row("nationality").get
        val DateValue(date) = row("date").get
        SwimmingRecord(event, time, name, nation, date)
      }
    )

    assert(fastGuyRec === selectResults(0))
    assert(slowGuyRec === selectResults(1))

  }

}
