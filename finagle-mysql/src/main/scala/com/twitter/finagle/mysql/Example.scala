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
    case class City(id: Int, name: String, date: Timestamp)

    // Select Query (Not using prepared statements)
    client.select("SELECT * FROM cities WHERE id in (1,2,3,7)") { row =>

      // deconstructing a Value in this way will throw a runtime match error
      // if valueOf returns anything but the Value on the left hand side.
      val IntValue(id) = row.valueOf("id").getOrElse(IntValue(-1))
      val StringValue(name) = row.valueOf("name").getOrElse(StringValue(""))
      val TimestampValue(ts) = row.valueOf("dateadded").getOrElse(TimestampValue(new Timestamp(0)))
      City(id, name, ts)
      
    } onSuccess {
      seq => seq.foreach(println)
    } onFailure {
      case e => e.printStackTrace()
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