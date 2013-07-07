package com.twitter.finagle.postgres.integration

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import com.twitter.finagle.postgres.{OK, Client}
import com.twitter.finagle.postgres.protocol.{PgRequest, Query, PgResponse, ServerError}
import com.twitter.logging.Logger
import com.twitter.util.{Future, Await}

case class User(email: String, name: String)

@RunWith(classOf[JUnitRunner])
class IntegrationSpec extends Specification {

  import com.twitter.logging.config._

  val config = new LoggerConfig {
    node = ""
    level = Logger.DEBUG
    handlers = new ConsoleHandlerConfig {
    }
  }
  config()


  args(sequential = true)

  "Postgres client" >> {
    val client = Client("localhost:5432", "mkhadikov", Some("pass"), "contacts")
    val i = Await.result(client.executeUpdate("delete from users"))

    "select on empty table should return empty list" in {
      val f = client.select("select  * from users") {row =>
        User(row.get[String]("email"), row.get[String]("name"))
      }

      Await.result(f).size === 0
    }
    "Empty query" in {
      val f = client.select("") {row =>
        User(row.get[String]("email"), row.get[String]("name"))
      }

      Await.result(f).size === 0
    }
    "Prepared statement select on empty table should return empty list" in {
      val f = for {
        prep <- client.prepare("select * from users where email = $1")
        users <- prep.select("mickey@mouse.com") {
          row => User(row.get[String]("email"), row.get[String]("name"))
        }
      } yield users

      Await.result(f).size === 0
    }

    "inserting item should work" in {
      val fi = client.executeUpdate("insert into users(email, name) values ('mickey@mouse.com', 'Mickey Mouse')," +
        " ('bugs@bunny.com', 'Bugs Bunny')")

      Await.result(fi) === OK(2)
    }

    "select query in prepared statement should work fine" in {
      val f = for {
        prep <- client.prepare("select * from users where email=$1 and name=$2")
        users <- prep.select("mickey@mouse.com", "Mickey Mouse") {
          row => User(row.get[String]("email"), row.get[String]("name"))
        }
      } yield users


      Await.result(f).size === 1
      Await.result(f).head.name === "Mickey Mouse"
    }


    "prepared statement with wrong params must fail" in {
      val f = for {
        prep <- client.prepare("select * from users where email=$1 and name=$2")
        users <- prep.select("one param only") {
          row => User(row.get[String]("email"), row.get[String]("name"))
        }
      } yield users

      Await.result(f) must throwA[ServerError]
    }

    "deleting item should work" in {
      val fd = client.executeUpdate("delete from users where email='bugs@bunny.com'")
      Await.result(fd) === OK(1)

      val f = client.select("select  * from users") { row =>
        User(row.get[String]("email"), row.get[String]("name"))
      }

      Await.result(f).size === 1
    }

    "updating item should work" in {
      val fd = client.executeUpdate("update users set name = 'Michael Mouse' where email='mickey@mouse.com'")
      Await.result(fd) === OK(1)

    }

    "updating item with prepared statement should work" in {
      val fu = for {
        prep <- client.prepare("update users set name=$1, email=$2 where email='mickey@mouse.com'")
        res <- prep.exec("Mr. Michael Mouse", "mr.mouse@mouse.com")
      } yield res

      Await.result(fu) === OK(1)

      val f = client.select("select  * from users") {
        row =>
          User(row.get[String]("email"), row.get[String]("name"))
      }

      Await.result(f).size === 1
      Await.result(f).head.name === "Mr. Michael Mouse"
      Await.result(f).head.email === "mr.mouse@mouse.com"
    }


    "inserting item with prepared statement should work" in {
      val fi = for {
        prep <- client.prepare("insert into users(email, name) values ($1, $2)")
        one <- prep.exec("Daisy Duck", "daisy@duck.com")
        two <- prep.exec("Minnie Mouse", "ms.mouse@mouse.com")
      } yield one.affectedRows + two.affectedRows

      Await.result(fi) === 2

      val f = client.select("select  * from users") {
        row =>
          User(row.get[String]("email"), row.get[String]("name"))
      }

      Await.result(f).size === 3
    }

    "wrong query should throw exception" in {
      val fd = client.executeUpdate("this is wrong query")
      Await.result(fd) must throwA[ServerError]
    }
  }

}

