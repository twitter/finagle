package com.twitter.finagle.postgres.integration

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import com.twitter.finagle.postgres.Client
import com.twitter.finagle.postgres.protocol.OK
import com.twitter.finagle.postgres.protocol.ServerError

case class User(email: String, name: String)

@RunWith(classOf[JUnitRunner])
class IntegrationSpec extends Specification {

  args(sequential = true)
  val on = false // only manual running

  if (on) {
    "Postgres client" >> {
      val client = Client("localhost:5432", "mkhadikov", Some("pass"), "contacts")
      val i = client.execute("delete from users").get

      "select on empty table should return empty list" in {
        val f = client.select("select  * from users") { row =>
          User(row.getString("email"), row.getString("name"))
        }

        f.get.size === 0
      }

      "inserting item should work" in {
        val fi = client.execute("insert into users(email, name) values ('mickey@mouse.com', 'Mickey Mouse')," +
          " ('bugs@bunny.com', 'Bugs Bunny')")

        fi.get === OK(2)
      }

      "deleting item should work" in {
        val fd = client.execute("delete from users where email='bugs@bunny.com'")
        fd.get === OK(1)

        val f = client.select("select  * from users") { row =>
          User(row.getString("email"), row.getString("name"))
        }

        f.get.size === 1
      }

      "updating item should work" in {
        val fd = client.execute("update users set name = 'Michael Mouse' where email='mickey@mouse.com'")
        fd.get === OK(1)

        val f = client.select("select  * from users") { row =>
          User(row.getString("email"), row.getString("name"))
        }

        f.get.size === 1
        f.get.head.name === "Michael Mouse"
      }

      "wrong query should throw exception" in {
        val fd = client.execute("this is wrong query")
        fd.get must throwA[ServerError]
      }
    }
  }

}

