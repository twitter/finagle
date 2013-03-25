package com.twitter.finagle.exp.mysql.integration

import com.twitter.finagle.exp.mysql._
import com.twitter.finagle.exp.mysql.protocol._
import com.twitter.util.NonFatal
import java.net.{ServerSocket, BindException}
import java.sql.Date
import java.util.logging.{Level, Logger}
import java.util.Properties
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import scala.collection.mutable

object ConnectionSettings {
  private val logger = Logger.getLogger("fingle-mysql-test")

  val p = new Properties
  try {
    val resource = getClass.getResource("/integration.properties")
    if (resource == null)
      logger.log(Level.WARNING, "integration.properties not found")
    else
      p.load(resource.openStream())
  } catch {
    case NonFatal(e) =>
      logger.log(Level.WARNING, "Exception while loading integration.properties", e)
  }

  // Requires mysql running @ localhost:3306
  // It's likely that mysqld is running if 3306 bind fails.
  val isAvailable = try {
    val socket = new ServerSocket(3306)
    socket.close()
    false
  } catch {
    case e: BindException => true
  }
}

object Connection {
  private val logger = Logger.getLogger("fingle-mysql-test")

  val client: Option[Client] = if (ConnectionSettings.isAvailable) {
    logger.log(Level.INFO, "Attempting to connect to mysqld @ localhost:3306")
    val username = ConnectionSettings.p.getProperty("username", "<user>")
    val password = ConnectionSettings.p.getProperty("password", "<password>")
    val db = ConnectionSettings.p.getProperty("db", "test")
    Some(Client("localhost:3306", username, password, db))
  } else {
    None
  }

  private[this] val prepared = mutable.Map[String, PreparedStatement]()
  def prepare(sql: String): Option[PreparedStatement] =
    if (prepared.contains(sql))
      prepared.get(sql)
    else
      client map { c =>
        val ps = c.prepare(sql).get
        prepared += (sql -> ps)
        ps
      }

  def close() = {
    client map { c =>
      prepared map { case (_, st) => c.closeStatement(st) }
      c.close()
    }
  }
}

case class SwimmingRecord(
  event: String,
  time: Float,
  name: String,
  nationality: String,
  date: Date
) extends {
  override def toString = {
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

  val allRecords = List[SwimmingRecord](
    SwimmingRecord("50 m freestyle", 20.91F, "Cesar Cielo", "Brazil", Date.valueOf("2009-12-18")),
    SwimmingRecord("100 m freestyle", 46.91F, "Cesar Cielo", "Brazil", Date.valueOf("2009-08-02")),
    SwimmingRecord("50 m backstroke", 24.04F, "Liam Tancock", "Great Britain", Date.valueOf("2009-08-02")),
    SwimmingRecord("100 m backstroke", 51.94F, "Aaron Peirsol", "United States", Date.valueOf("2009-07-08")),
    SwimmingRecord("50 m butterfly", 22.43F, "Rafael Munoz", "Spain", Date.valueOf("2009-05-05")),
    SwimmingRecord("100 m butterfly", 49.82F, "Michael Phelps", "United States", Date.valueOf("2009-07-29"))
  )

  def countRecordsWith(name: String): Int = allRecords filter { _.name == name } size
}

class ClientTest extends FunSuite with BeforeAndAfterAll {

  for (c <- Connection.client) {
    test("Ping Server") {
      val pingResult = c.ping.get
      expectResult(true) { pingResult.isInstanceOf[OK] }
    }

    test("Create Table") {
      val createResult = c.query(SwimmingRecord.schema).get
      expectResult(true) { createResult.isInstanceOf[OK] }
    }

    test("Insert") {
      val sql =
      """INSERT INTO `finagle-mysql-test` (`event`, `time`, `name`, `nationality`, `date`)
         VALUES %s;""".format(SwimmingRecord.allRecords.mkString(", "))

      val insertResult = c.query(sql).get
      val OK(_, insertid, _, _, _) = insertResult.asInstanceOf[OK]
      expectResult(true) { insertResult.isInstanceOf[OK] }
    }

    test("Select") {
      val selectResult = c.select("SELECT * FROM `finagle-mysql-test`") { row =>
        val StringValue(event) = row("event").get
        val FloatValue(time) = row("time").get
        val StringValue(name) = row("name").get
        val StringValue(nation) = row("nationality").get
        val DateValue(date) = row("date").get
        SwimmingRecord(event, time, name, nation, date)
      }.get

      var i = 0
      for (res <- selectResult) {
        expectResult(res) { SwimmingRecord.allRecords(i) }
        i += 1
      }
    }

    val prepareQuery = "SELECT COUNT(*) AS 'numRecords' FROM `finagle-mysql-test` WHERE `name` LIKE ?"
    // this assumption is made based on the prepareQuery.
    def extractRow(r: Result) = r.asInstanceOf[ResultSet].rows(0)

    test("Prepared Statement with 1 parameter, execute twice") {
      // choose random record from local list
      val randomIdx = math.floor(math.random * (SwimmingRecord.allRecords.size-1)).toInt
      val recordName = SwimmingRecord.allRecords(randomIdx).name

      // query using the prepared statement
      val ps = c.prepare(prepareQuery).get
      ps.parameters = Array(recordName)
      val result1 = c.execute(ps).get
      val result2 = c.execute(ps).get

      val expectedRes = LongValue(SwimmingRecord.countRecordsWith(recordName))
      expectResult(expectedRes) {
        val row = extractRow(result1)
        row("numRecords").get
      }

      expectResult(expectedRes) {
        val row = extractRow(result2)
        row("numRecords").get
      }
    }
  }

  override def afterAll() { Connection.close() }
}