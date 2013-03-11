package com.twitter.finagle.exp.mysql

import com.twitter.conversions.time._
import com.twitter.finagle.exp.mysql._
import com.twitter.finagle.exp.mysql.protocol._
import com.twitter.util.{Future, Try}
import java.sql.Date
import java.util.logging.{Logger, Level}

case class SwimmingRecord(
  event: String,
  time: Float,
  name: String,
  nationality: String,
  date: Date
) {

  def toArray = Array(event, time, name, nationality, date)

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

object Main {
  def main(args: Array[String]): Unit = {
    val options = parseArgs(Map(), args.toList)
    val host = options.getOrElse("host", "localhost").asInstanceOf[String]
    val port = options.getOrElse("port", 3306).asInstanceOf[Int]
    val username = options.getOrElse("username", "<user>").asInstanceOf[String]
    val password = options.getOrElse("password", "<password>").asInstanceOf[String]
    val dbname = options.getOrElse("database", "test").asInstanceOf[String]

    val client = Client(host+":"+port, username, password, dbname, Level.OFF)
    if (createTable(client) && insertValues(client)) {
      val query = "SELECT * FROM `finagle-mysql-example` WHERE `date` BETWEEN '2009-06-01' AND '2009-8-31'"
      val qres: Future[Seq[_]] = client.select(query) { row =>
        // row(...) returns an Option[Value]
        val event = row("event") map {
          case StringValue(s) => s
          case _ => "Default"
        }

        val nationality = row("nationality") map {
          case StringValue(s) => s
          case _ => "Default"
        }

        val date = row("date") map {
          case DateValue(d) => d
          case _ => new Date(0)
        }

        // Of course this could cause a runtime error if there isn't a field name
        // "time" or if the Value at the field named time is not of type
        // FloatValue(_)
        val FloatValue(time) = row("time").get

        val StringValue(name) = row("name").get

        (name, time)
      }

      qres onSuccess { res =>
        res.foreach(println)
      } onFailure {
        e => println("Failed query with %s".format(e))
      }
    }
  }

  def createTable(client: Client): Boolean = {
    val res = client.query(SwimmingRecord.createTableSQL).get(1.second) onFailure {
      case e => println("Failed with %s when attempting to create table".format(e))
    }

    res.isReturn
  }

  def insertValues(client: Client): Boolean = {
    val insertSQL = "INSERT INTO `finagle-mysql-example` (`event`, `time`, `name`, `nationality`, `date`) VALUES (?,?,?,?,?)"
    var insertResults: Seq[Try[_]] = Seq()
    // create a prepared statement on the server
    // and insert swimming records.
    val preparedFuture = client.prepare(insertSQL).get(1.second)
    preparedFuture onSuccess { ps =>
      insertResults = for (r <- SwimmingRecord.records) yield {
        ps.parameters = r.toArray
        client.execute(ps).get(1.second) onFailure {
          case e => println("Failed with %s when attempting to insert %s".format(e, r))
        }
      }
    } onFailure {
      case e => println("Failed with %s when attempting to create a prepared statement".format(e))
    }

    // close prepared statement on the server
    for(ps <- preparedFuture) {
      client.closeStatement(ps).get(1.second) onFailure {
        e => println("Unable to close PreparedStatement: %s".format(e))
      }
    }

    preparedFuture.isReturn && insertResults.forall(_.isReturn)
  }

  def parseArgs(parsed: Map[String, Any], args: List[String]): Map[String, Any] = args match {
    case Nil => parsed
    case "-host" :: value :: tail =>
      parseArgs(parsed + ("host" -> value), tail)
    case "-port" :: value :: tail =>
      parseArgs(parsed + ("port" -> value.toInt), tail)
    case "-u" :: value :: tail =>
      parseArgs(parsed + ("username" -> value), tail)
    case "-p" :: value :: tail =>
      parseArgs(parsed + ("password" -> value), tail)
    case "-database" :: value :: tail =>
      parseArgs(parsed + ("database" -> value), tail)
    case unknown :: tail => parsed
  }
}