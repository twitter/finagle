package com.twitter.finagle.example.mysql

import com.twitter.app.App
import com.twitter.util.{Await, Future}
import com.twitter.finagle.Mysql
import com.twitter.finagle.mysql._
import java.net.InetSocketAddress
import java.sql.Date

case class SwimmingRecord(event: String, time: Float, name: String, nationality: String, date: Date)

object SwimmingRecord {
  val createTableSQL =
    """CREATE TABLE IF NOT EXISTS `finagle-mysql-example` (
    `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
    `event` varchar(30) DEFAULT NULL,
    `time` float DEFAULT NULL,
    `name` varchar(40) DEFAULT NULL,
    `nationality` varchar(20) DEFAULT NULL,
    `date` date DEFAULT NULL,
    PRIMARY KEY (`id`)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8"""

  val records = List(
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
    )
  )
}

object Example extends App {
  val host = flag("server", new InetSocketAddress("localhost", 3306), "mysql server address")
  val username = flag("username", "<user>", "mysql username")
  val password = flag("password", "<password>", "mysql password")
  val dbname = flag("database", "test", "default database to connect to")

  def main(): Unit = {
    val client = Mysql.client
      .withCredentials(username(), password())
      .withDatabase(dbname())
      .newRichClient(s"${host().getHostName}:${host().getPort}")

    val resultFuture = for {
      _ <- createTable(client)
      _ <- insertValues(client)
      r <- selectQuery(client)
    } yield r

    resultFuture
      .onSuccess { seq => seq.foreach(println) }
      .onFailure { e => println(e) }
      .ensure {
        client.query("DROP TABLE IF EXISTS `finagle-mysql-example`").ensure {
          client.close()
        }
      }

    Await.ready(resultFuture)
  }

  def createTable(client: Client): Future[OK] = {
    client.modify(SwimmingRecord.createTableSQL)
  }

  def insertValues(client: Client): Future[Seq[OK]] = {
    val insertSQL =
      "INSERT INTO `finagle-mysql-example` (`event`, `time`, `name`, `nationality`, `date`) VALUES (?,?,?,?,?)"
    val ps = client.prepare(insertSQL)
    val insertResults = SwimmingRecord.records.map { r =>
      ps.modify(r.event, r.time, r.name, r.nationality, r.date)
    }
    Future.collect(insertResults)
  }

  def selectQuery(client: Client): Future[Seq[_]] = {
    val query =
      "SELECT * FROM `finagle-mysql-example` WHERE `date` BETWEEN '2009-06-01' AND '2009-08-31'"
    client.select(query) { row =>
      val date = row.javaSqlDateOrNull("date")
      val name = row.stringOrNull("name")
      val time = row.floatOrZero("time")

      (name, time, date)
    }
  }
}
