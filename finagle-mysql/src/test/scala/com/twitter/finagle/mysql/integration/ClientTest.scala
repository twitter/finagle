package com.twitter.finagle.exp.mysql.integration

import com.twitter.finagle.exp.Mysql
import com.twitter.finagle.exp.mysql._
import com.twitter.util.Await
import java.sql.Date
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

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
}

@RunWith(classOf[JUnitRunner])
class ClientTest extends FunSuite with IntegrationClient {
  import SwimmingRecord._
  for (c <- client) {
    test("failed auth") {
      try {
        Await.result(Mysql.newRichClient("localhost:3306").ping)
        fail("Expected an error when using an unauthenticated client")
      } catch {
        // Expected Access Denied Error Code
        case ServerError(code, _, _) => assert(code == 1045)
      }
    }

    test("ping") {
      val pingResult = Await.result(c.ping)
      assert(pingResult.isInstanceOf[OK])
    }

    test("query: create a table") {
      val createResult = Await.result(c.query(schema))
      assert(createResult.isInstanceOf[OK])
    }

    test("query: insert values") {
      val sql = """INSERT INTO `finagle-mysql-test` (`event`, `time`, `name`, `nationality`, `date`)
         VALUES %s;""".format(allRecords.mkString(", "))

      val insertResult = Await.result(c.query(sql))
      val OK(_, insertid, _, _, _) = insertResult.asInstanceOf[OK]
      assert(insertResult.isInstanceOf[OK])
      assert(insertid == 1)
    }

    test("query: select values") {
      val selectResult = Await.result(c.select("SELECT * FROM `finagle-mysql-test`") { row =>
        val StringValue(event) = row("event").get
        val FloatValue(time) = row("time").get
        val StringValue(name) = row("name").get
        val StringValue(nation) = row("nationality").get
        val DateValue(date) = row("date").get
        SwimmingRecord(event, time, name, nation, date)
      })

      var i = 0
      for (res <- selectResult) {
        assert(allRecords(i) == res)
        i += 1
      }
    }

    test("prepared statement") {
      val prepareQuery = "SELECT COUNT(*) AS 'numRecords' FROM `finagle-mysql-test` WHERE `name` LIKE ?"
      def extractRow(r: Result) = r.asInstanceOf[ResultSet].rows(0)
      val ps = c.prepare(prepareQuery)
      for (i <- 0 to 10) {
        val randomIdx = math.floor(math.random * (allRecords.size-1)).toInt
        val recordName = allRecords(randomIdx).name
        val expectedRes = LongValue(allRecords.filter(_.name == recordName).size)
        val res = ps.select(recordName)(identity)
        val row = Await.result(res)(0)
        assert(row("numRecords").get == expectedRes)
      }
    }
  }
}
