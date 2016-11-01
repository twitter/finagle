package com.twitter.finagle.postgres.integration

import com.twitter.finagle.Postgres
import com.twitter.finagle.postgres.{Client, Spec}
import com.twitter.util.{Await, Future}

class TransactionSpec extends Spec {
  for {
    hostPort <- sys.env.get("PG_HOST_PORT")
    user <- sys.env.get("PG_USER")
    password = sys.env.get("PG_PASSWORD")
    dbname <- sys.env.get("PG_DBNAME")
    useSsl = sys.env.getOrElse("USE_PG_SSL", "0") == "1"
  } yield {

    val client = Postgres.Client()
      .withCredentials(user, password)
      .database(dbname)
      .conditionally(useSsl, _.withTransport.tlsWithoutValidation)
      .newRichClient(hostPort)

    Await.result(client.query(
      """
        |DROP TABLE IF EXISTS transaction_test;
        |CREATE TABLE transaction_test(id integer primary key);
      """.stripMargin))

    "A postgres transaction" should {

      "commit if the transaction future is successful" in {
        Await.result {
          client.inTransaction {
            c => for {
              _ <- c.prepareAndExecute("DELETE FROM transaction_test")
              _ <- c.prepareAndExecute("INSERT INTO transaction_test VALUES(1)")
              _ <- c.prepareAndExecute("INSERT INTO transaction_test VALUES(2)")
            } yield ()
          }
        }
        val count = Await.result(client.prepareAndQuery("SELECT COUNT(*)::int4 AS count FROM transaction_test WHERE id IN (1,2)") {
          row => row.get[Int]("count")
        }.map(_.head))
        assert(count == 2)
      }

      "rollback the transaction if the transaction future fails" in {
        val failed = client.inTransaction {
          c => for {
            _ <- c.prepareAndExecute("DELETE FROM transaction_test")
            _ <- c.prepareAndExecute("INSERT INTO transaction_test VALUES(3)")
            _ <- c.prepareAndExecute("INSERT INTO transaction_test VALUES(4)")
            _ <- Future.exception(new Exception("Roll it back!"))
            _ <- c.prepareAndExecute("INSERT INTO transaction_test VALUES(5)")
          } yield ()
        }.liftToTry

        val failedResult = Await.result(failed)

        val inTable = Await.result(client.prepareAndQuery("SELECT * FROM transaction_test") {
          row => row.get[Int]("id")
        }).toList.sorted
        assert(inTable == List(1, 2))
      }

    }
  }
}
