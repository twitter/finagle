package com.twitter.finagle.mysql.integration

import com.twitter.finagle.mysql._
import com.twitter.finagle.mysql.protocol._
import java.net.{ServerSocket, BindException}
import java.sql.Date
import org.specs.SpecificationWithJUnit
import scala.collection.mutable

object Connection {
  // Requires mysql running @ localhost:3306
  // It's likely that mysqld is running if 3306 bind fails
  val isAvailable = try {
    new ServerSocket(3306)
    false
  } catch {
    case e: BindException => true
  }

  val client: Option[Client] = if (isAvailable) {
    println("Attempting to connect to mysqld @ localhost:3306")
    val username = readLine("Enter username:")
    val password = readLine("\nEnter password:")
    val db = readLine("\nEnter database name:")
    println("")
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
      prepared map { kv => c.closeStatement(kv._2) }
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
  val currentRecords = List[SwimmingRecord](
    SwimmingRecord("50 m freestyle", 20.91F, "Cesar Cielo", "Brazil", Date.valueOf("2009-12-18")),
    SwimmingRecord("100 m freestyle", 46.91F, "Cesar Cielo", "Brazil", Date.valueOf("2009-08-02")),
    SwimmingRecord("50 m backstroke", 24.04F, "Liam Tancock", "Great Britain", Date.valueOf("2009-08-02")),
    SwimmingRecord("100 m backstroke", 51.94F, "Aaron Peirsol", "United States", Date.valueOf("2009-07-08")),
    SwimmingRecord("50 m butterfly", 22.43F, "Rafael Munoz", "Spain", Date.valueOf("2009-05-05")),
    SwimmingRecord("100 m butterfly", 49.82F, "Michael Phelps", "United States", Date.valueOf("2009-07-29"))
  )

  def numRecords(name: String): Int = currentRecords filter { _.name == name } size
}

class ClientSpec extends SpecificationWithJUnit {
  import SwimmingRecord._

  "MySQL Client" should {
    val c = Connection.client
    if(!c.isEmpty) {

      val client = c.get

      "Ping" in {
        val pingRes = client.ping.get
        pingRes.isInstanceOf[OK] mustEqual true
      }

      "Run basic queries" in {
        "Create Table" in {
          val sql = """CREATE TEMPORARY TABLE IF NOT EXISTS `finagle-mysql-test` (
            `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
            `event` varchar(30) DEFAULT NULL,
            `time` float DEFAULT NULL,
            `name` varchar(40) DEFAULT NULL,
            `nationality` varchar(20) DEFAULT NULL,
            `date` date DEFAULT NULL,
            PRIMARY KEY (`id`)
          ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""

          val createRes = client.query(sql).get
          createRes.isInstanceOf[OK] mustEqual true
        }

        "Insert" in {
          val sql = """INSERT INTO `finagle-mysql-test` (`event`, `time`, `name`, `nationality`, `date`)
            VALUES
              %s;""".format(currentRecords.mkString(", "))

          val insertRes = client.query(sql).get
          insertRes.isInstanceOf[OK] mustEqual true
        }

        "Select" in {
          val selectRes = client.select("SELECT * FROM `finagle-mysql-test`") { row =>
            val StringValue(event) = row("event").get
            val FloatValue(time) = row("time").get
            val StringValue(name) = row("name").get
            val StringValue(nation) = row("nationality").get
            val DateValue(date) = row("date").get
            SwimmingRecord(event, time, name, nation, date)
          }.get

          var i = 0
          for (res <- selectRes) {
            currentRecords(i) mustEqual res
            i += 1
          }
        }
      }

      "Run queries using prepared statements" in {
        val p = Connection.prepare("SELECT COUNT(*) AS 'numRecords' FROM `finagle-mysql-test` WHERE `name` LIKE ?")
        if(!p.isEmpty) {
          val ps = p.get
          val randomIdx = math.floor(math.random * (currentRecords.size-1)).toInt
          val recordHolder = currentRecords(randomIdx).name

          // this assumption is made based on the query sent
          // to prepare.
          def extractRow(r: Result) = r.asInstanceOf[ResultSet].rows(0)

          "execute #1" in {
            ps.parameters = Array(recordHolder)
            val row = extractRow(client.execute(ps).get)
            val result = row("numRecords").get
            result mustEqual LongValue(numRecords(recordHolder))

            "again" in {
              val row = extractRow(client.execute(ps).get)
              row("numRecords").get mustEqual result
            }
          }

          "execute #2" in {
            ps.parameters = Array(recordHolder)
            val execRes = client.execute(ps).get.asInstanceOf[ResultSet]
            val row = execRes.rows(0)
            val result = row("numRecords").get
            result mustEqual LongValue(numRecords(recordHolder))

            "again" in {
              val row = extractRow(client.execute(ps).get)
              row("numRecords").get mustEqual result
            }
          }
        }
      }

    } // if !client.isEmpty

    doLast(Connection.close())

  } // should
}