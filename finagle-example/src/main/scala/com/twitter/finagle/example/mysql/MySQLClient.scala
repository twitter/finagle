package com.twitter.finagle.example.mysql

import com.twitter.app.App
import com.twitter.util.Future
import com.twitter.finagle.exp.mysql._
import java.net.InetSocketAddress
import java.sql.Date
import java.util.logging.{Logger, Level}

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
  val createTableSQL =
  """CREATE TEMPORARY TABLE IF NOT EXISTS `finagle-mysql-example` (
    `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
    `event` varchar(30) DEFAULT NULL,
    `time` float DEFAULT NULL,
    `name` varchar(40) DEFAULT NULL,
    `nationality` varchar(20) DEFAULT NULL,
    `date` date DEFAULT NULL,
    PRIMARY KEY (`id`)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8"""

  val records = List(
    SwimmingRecord("50 m freestyle", 20.91F, "Cesar Cielo", "Brazil", Date.valueOf("2009-12-18")),
    SwimmingRecord("100 m freestyle", 46.91F, "Cesar Cielo", "Brazil", Date.valueOf("2009-08-02")),
    SwimmingRecord("50 m backstroke", 24.04F, "Liam Tancock", "Great Britain", Date.valueOf("2009-08-02")),
    SwimmingRecord("100 m backstroke", 51.94F, "Aaron Peirsol", "United States", Date.valueOf("2009-07-08")),
    SwimmingRecord("50 m butterfly", 22.43F, "Rafael Munoz", "Spain", Date.valueOf("2009-05-05")),
    SwimmingRecord("100 m butterfly", 49.82F, "Michael Phelps", "United States", Date.valueOf("2009-07-29"))
  )
}

object MySQLClient extends App {
  val host = flag("server", new InetSocketAddress("localhost", 3306), "mysql server address")
  val username = flag("username", "<user>", "mysql username")
  val password = flag("password", "<password>", "mysql password")
  val dbname = flag("database", "test", "default database to connect to")

  def main() {
    val client = Client(host().getHostName+":"+host().getPort, username(), password(), dbname(), Level.OFF)
    val resultFuture = for {
      _ <- createTable(client)
      _ <- insertValues(client)
      r <- selectQuery(client)
    } yield r

    resultFuture onSuccess { seq =>
      seq foreach println
    } onFailure { e =>
      println(e)
    } ensure {
      client.close()
    }
  }

  def createTable(client: Client): Future[Result] = {
    client.query(SwimmingRecord.createTableSQL)
  }

  def insertValues(client: Client): Future[Seq[Result]] = {
    val insertSQL = "INSERT INTO `finagle-mysql-example` (`event`, `time`, `name`, `nationality`, `date`) VALUES (?,?,?,?,?)"
    client.prepare(insertSQL) flatMap { ps =>
      val insertResults = SwimmingRecord.records map { r =>
        ps.parameters = Array(r.event, r.time, r.name, r.nationality, r.date)
        client.execute(ps)
      }
      Future.collect(insertResults) ensure {
        client.closeStatement(ps)
      }
    }
  }

  def selectQuery(client: Client): Future[Seq[_]] = {
    val query = "SELECT * FROM `finagle-mysql-example` WHERE `date` BETWEEN '2009-06-01' AND '2009-8-31'"
    client.select(query) { row =>
      val StringValue(event) = row("event").get
      val DateValue(date) = row("date").get
      val StringValue(name) = row("name").get
      val time = row("time") map {
        case FloatValue(f) => f
        case _ => 0.0F
      } get

      (name, time)
    }
  }
}