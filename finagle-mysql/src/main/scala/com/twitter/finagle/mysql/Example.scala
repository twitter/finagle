import com.twitter.finagle.mysql._
import com.twitter.finagle.mysql.protocol._
import com.twitter.util.Future
import java.net.InetSocketAddress
import java.sql.Timestamp

object Main {
  def main(args: Array[String]): Unit = {
    val options = parseArgs(Map(), args.toList)
    val host = options.getOrElse("host", "localhost").asInstanceOf[String]
    val port = options.getOrElse("port", 3306).asInstanceOf[Int]
    val username = options.getOrElse("username", "<user>").asInstanceOf[String]
    val password = options.getOrElse("password", "<password>").asInstanceOf[String]
    val dbname = "test"

    val client = Client(host+":"+port, username, password, dbname)
    case class City(id: Option[Int], name: Option[String], date: Option[Timestamp])

    // Select Query (Not using prepared statements) */
    client.select("SELECT * FROM cities WHERE id in (1,2,3,7)") { row =>

      val id = row.valueOf("id") map {
        case IntValue(i) => i
        // case NullValue =>
        // case EmptyValue =>
        case _ => -1
      }

      val name = row.valueOf("name") map {
        case StringValue(s) => s
        case _ => ""
      }

      val dateAdded = row.valueOf("dateadded") map {
        case TimestampValue(ts) => ts
        case _ => new Timestamp(0)
      }

      City(id, name, dateAdded)

    } onSuccess {
      seq => seq.foreach(println)
    } onFailure {
      case e => e.printStackTrace()
    }

    // Prepared Statements
    /*client.prepareAndSelect("SELECT * FROM cities WHERE id in (?)", (1,3,7)) { row => 
      println(row.values)
    } onSuccess {
      case (ps, seq) => seq.foreach(println)
    } onFailure {
      e => e.printStackTrace()
    }*/
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
    case unknown :: tail => parsed
  }
}