package com.twitter.finagle

import com.twitter.finagle.postgresql.Messages
import com.twitter.util.Await
import org.specs2.mutable.Specification

class BasicSpec extends Specification {

  "client" should {
    "connct" in {
      val client = PostgreSQL.Client()
        .withCredentials("postgres", "secret")
        .withDatabase("test")
        .newService("127.0.0.1:5432")
      println(Await.result(client(Messages.Sync)))
      true
    }
  }

}
