package com.twitter.finagle.mysql.integration

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.mysql.QueryRequest
import com.twitter.util.{Await, Awaitable}
import org.scalatest.{FunSuite, OptionValues}

class ConnectionInitSqlTest extends FunSuite with IntegrationClient with OptionValues {

  private[this] def await[T](t: Awaitable[T]): T = Await.result(t, 5.seconds)

  override def configureClient(username: String, password: String, db: String) = {
    super
      .configureClient(username, password, db)
      .withConnectionInitRequest(QueryRequest("SET @CIS_VAR = 99"))
  }

  test("get connection init request variable") {
    val theClient = client.orNull
    val result = await(theClient.select("SELECT @CIS_VAR as v") { row =>
      row.getLong("v")
    })
    assert(result.headOption.value.value == 99L)
  }
}
