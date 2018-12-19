import com.twitter.finagle.Mysql
import com.twitter.finagle.mysql.{ResultSet, Row, QueryRequest, LongValue, IntValue}
import com.twitter.util.Await
import com.twitter.finagle.client.DefaultPool
import com.twitter.conversions.DurationOps._

object Shared {
  //#processRow
  def processRow(row: Row): Option[Long] =
    row.getLong("product")
  //#processRow
}

object ServiceFactoryExample {
  import Shared._

  //#client
  val client = Mysql.client
    .withCredentials("<user>", "<password>")
    .withDatabase("test")
    .configured(DefaultPool
      .Param(low = 0, high = 10, idleTime = 5.minutes, bufferSize = 0, maxWaiters = Int.MaxValue))
    .newClient("127.0.0.1:3306")
  //#client

  def main(args: Array[String]): Unit = {
    //#query0
    val product = client().flatMap { service =>
      // `service` is checked out from the pool.
      service(QueryRequest("SELECT 5*5 AS `product`"))
        .map {
          case rs: ResultSet => rs.rows.map(processRow)
          case _ => Seq.empty
        }.ensure {
          // put `service` back into the pool.
          service.close()
        }
    }
    //#query0

    println(Await.result(product))
  }
}

object RichExample {
  import Shared._

  //#richClient
  val richClient = Mysql.client
    .withCredentials("<user>", "<password>")
    .withDatabase("test")
    .configured(DefaultPool
      .Param(low = 0, high = 10, idleTime = 5.minutes, bufferSize = 0, maxWaiters = Int.MaxValue))
    .newRichClient("127.0.0.1:3306")
  //#richClient

  def main(args: Array[String]): Unit = {
    //#query1
    val product = richClient.select("SELECT 5*5 AS `product`")(processRow)
    //#query1
    println(Await.result(product))
  }
}
