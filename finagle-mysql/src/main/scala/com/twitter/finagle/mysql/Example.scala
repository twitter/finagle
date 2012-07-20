import com.twitter.finagle.mysql._
import com.twitter.finagle.mysql.protocol._
import com.twitter.util.Future
import java.net.InetSocketAddress
import java.sql.{Date, Time, Timestamp}

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

    //Basic Queries
    /*client.select("SELECT * FROM cities WHERE id in (?)", (1,2)) { row =>
      City(row.getInt("id"), row.getString("name"), row.getTimestamp("dateadded"))
    } onSuccess {
      result => println(result)
    } onFailure {
      case e => e.printStackTrace()
    }*/


    //Prepared Statements
    client.prepareAndSelect("SELECT * FROM cities WHERE id in (?)", (1,2,3)) { row => 
      City(row.getInt("id"), row.getString("name"), row.getTimestamp("dateadded"))
    } onSuccess {
      case (ps, seq) => println(seq)
    } onFailure {
      e => e.printStackTrace()
    }
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