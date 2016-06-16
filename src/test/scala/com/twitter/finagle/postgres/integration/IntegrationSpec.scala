package com.twitter.finagle.postgres.integration

import java.sql.Timestamp

import com.twitter.finagle.postgres.codec.ServerError
import com.twitter.finagle.postgres.{Row, OK, Spec, Client}
import com.twitter.util.{Duration, Await}

object IntegrationSpec {
  val pgHostPort = "localhost:5432"
  val pgUser = "finagle_tester"
  val pgPassword = "abc123"
  val pgDbName = "finagle_postgres_test"

  val pgTestTable = "finagle_test"
}

/*
 * Note: For these to work, you need to have:
 *
 * (1) Postgres running locally on port 5432
 * (2) A database called "finagle_postgres_test"
 * (3) A user named "finagle_tester" with a password of "abc123" and full access to the previous DB
 *
 * If these are conditions are met, set the environment variable RUN_PG_INTEGRATION_TESTS to "1" before running pants.
 *
 * The tests can be run with SSL by also setting the USE_PG_SSL variable to "1".
 *
 * If the previous environment variable is not set, the tests will not be run.
 */
class IntegrationSpec extends Spec {
  def postgresAvailable: Boolean = {
    sys.env.get("RUN_PG_INTEGRATION_TESTS") == Some("1")
  }

  def useSsl: Boolean = {
    sys.env.get("USE_PG_SSL") == Some("1")
  }

  val queryTimeout = Duration.fromSeconds(2)

  def getClient = {
    Client(
      IntegrationSpec.pgHostPort,
      IntegrationSpec.pgUser,
      Some(IntegrationSpec.pgPassword),
      IntegrationSpec.pgDbName,
      useSsl = useSsl
    )
  }

  def cleanDb(client: Client): Unit = {
    val dropQuery = client.executeUpdate("DROP TABLE IF EXISTS %s".format(IntegrationSpec.pgTestTable))
    val response = Await.result(dropQuery, queryTimeout)

    response must equal(OK(1))

    val createTableQuery = client.executeUpdate(
      """
        |CREATE TABLE %s (
        | str_field VARCHAR(40),
        | int_field INT,
        | double_field DOUBLE PRECISION,
        | timestamp_field TIMESTAMP WITH TIME ZONE,
        | bool_field BOOLEAN
        |)
      """.stripMargin.format(IntegrationSpec.pgTestTable))
    val response2 = Await.result(createTableQuery, queryTimeout)

    response2 must equal(OK(1))
  }

  def insertSampleData(client: Client): Unit = {
    val insertDataQuery = client.executeUpdate(
      """
        |INSERT INTO %s VALUES
        | ('hello', 1234, 10.5, '2015-01-08 11:55:12-0800', TRUE),
        | ('hello', 5557, -4.51, '2015-01-08 12:55:12-0800', TRUE),
        | ('hello', 7787, -42.51, '2013-12-24 07:01:00-0800', FALSE),
        | ('goodbye', 4567, 15.8, '2015-01-09 16:55:12+0500', FALSE)
      """.stripMargin.format(IntegrationSpec.pgTestTable))

    val response = Await.result(insertDataQuery, queryTimeout)

    response must equal(OK(4))
  }

  "A postgres client" should {
    "insert and select rows" in {
      if (postgresAvailable) {
        val client = getClient
        cleanDb(client)
        insertSampleData(client)

        val selectQuery = client.select(
          "SELECT * FROM %s WHERE str_field='hello' ORDER BY timestamp_field".format(IntegrationSpec.pgTestTable)
        )(identity)

        val resultRows = Await.result(selectQuery, queryTimeout)

        resultRows.size must equal(3)

        // Spot check the first row
        val firstRow = resultRows(0)

        firstRow.getOption("str_field") must equal(Some("hello"))
        firstRow.getOption("int_field") must equal(Some(7787))
        firstRow.getOption("double_field") must equal(Some(-42.51))
        firstRow.getOption("timestamp_field") must equal(Some(new Timestamp(1387897260000L)))
        firstRow.getOption("bool_field") must equal(Some(false))
        firstRow.getOption("bad_column") must equal(None)
      }
    }

    "execute a select that returns nothing" in {
      if (postgresAvailable) {
        val client = getClient
        cleanDb(client)
        insertSampleData(client)

        val selectQuery = client.select(
          "SELECT * FROM %s WHERE str_field='xxxx' ORDER BY timestamp_field".format(IntegrationSpec.pgTestTable)
        )(identity)

        val resultRows = Await.result(selectQuery, queryTimeout)

        resultRows.size must equal(0)
      }
    }

    "update a row" in {
      if (postgresAvailable) {
        val client = getClient
        cleanDb(client)
        insertSampleData(client)

        val updateQuery = client.executeUpdate(
          "UPDATE %s SET str_field='hello_updated' where int_field=4567".format(IntegrationSpec.pgTestTable)
        )

        val response = Await.result(updateQuery, queryTimeout)

        response must equal(OK(1))

        val selectQuery = client.select(
          "SELECT * FROM %s WHERE str_field='hello_updated'".format(IntegrationSpec.pgTestTable)
        )(identity)

        val resultRows = Await.result(selectQuery, queryTimeout)

        resultRows.size must equal(1)
        resultRows(0).getOption("str_field") must equal(Some("hello_updated"))
      }
    }

    "delete rows" in {
      if (postgresAvailable) {
        val client = getClient
        cleanDb(client)
        insertSampleData(client)

        val updateQuery = client.executeUpdate(
          "DELETE FROM %s WHERE str_field='hello'".format(IntegrationSpec.pgTestTable)
        )

        val response = Await.result(updateQuery, queryTimeout)

        response must equal(OK(3))

        val selectQuery = client.select(
          "SELECT * FROM %s".format(IntegrationSpec.pgTestTable)
        )(identity)

        val resultRows = Await.result(selectQuery, queryTimeout)

        resultRows.size must equal(1)
        resultRows(0).getOption("str_field") must equal(Some("goodbye"))
      }
    }

    "select rows via a prepared query" in {
      if (postgresAvailable) {
        val client = getClient
        cleanDb(client)
        insertSampleData(client)

        val preparedQuery = client.prepareAndQuery("SELECT * FROM %s WHERE str_field=$1 AND bool_field=$2"
          .format(IntegrationSpec.pgTestTable), "hello", true)(identity)
        val resultRows = Await.result(
          preparedQuery,
          queryTimeout
        )

        resultRows.size must equal(2)
        resultRows.foreach {
          row =>
            row.getOption("str_field") must equal(Some("hello"))
            row.getOption("bool_field") must equal(Some(true))
        }
      }
    }
  
    "execute an update via a prepared statement" in {
      if (postgresAvailable) {
        val client = getClient
        cleanDb(client)
        insertSampleData(client)


        val preparedQuery = client.prepareAndExecute(
          "UPDATE %s SET str_field = $1 where int_field = 4567".format(IntegrationSpec.pgTestTable),
          "hello_updated"
        )
    
        val numRows = Await.result(preparedQuery)
    
        val resultRows = Await.result(client.select("SELECT * from %s WHERE str_field = 'hello_updated' AND int_field = 4567")(identity))

        resultRows.size must equal(numRows)
      }
    }
  
    "return rows from UPDATE...RETURNING" in {
      if (postgresAvailable) {
        val client = getClient
        cleanDb(client)
        insertSampleData(client)


        val preparedQuery = client.prepareAndQuery(
          "UPDATE %s SET str_field = $1 where int_field = 4567 RETURNING *".format(IntegrationSpec.pgTestTable),
          "hello_updated"
        )(identity)
    
        val resultRows = Await.result(preparedQuery)

        resultRows.size must equal(1)
        resultRows(0).get[String]("str_field") must equal("hello_updated")
      }
    }
  
    "return rows from DELETE...RETURNING" in {
      if (postgresAvailable) {
        val client = getClient
        cleanDb(client)
        insertSampleData(client)


        val preparedQuery = client.prepareAndQuery(
          "DELETE FROM %s where int_field = 4567 RETURNING *".format(IntegrationSpec.pgTestTable)
        )(identity)
    
        val resultRows = Await.result(preparedQuery)

        resultRows.size must equal(1)
        resultRows(0).get[String]("str_field") must equal("hello")
      }
    }
  
    "execute an UPDATE...RETURNING that updates nothing" in {
      if (postgresAvailable) {
        val client = getClient
        cleanDb(client)
        insertSampleData(client)


        val preparedQuery = client.prepareAndQuery(
          "UPDATE %s SET str_field = $1 where str_field = $2 RETURNING *".format(IntegrationSpec.pgTestTable),
          "hello_updated",
          "xxxx"
        )(identity)
    
        val resultRows = Await.result(preparedQuery)

        resultRows.size must equal(0)
      }
    }
  
    "execute a DELETE...RETURNING that deletes nothing" in {
      if (postgresAvailable) {
        val client = getClient
        cleanDb(client)
        insertSampleData(client)


        val preparedQuery = client.prepareAndQuery(
          "DELETE FROM %s WHERE str_field=$1".format(IntegrationSpec.pgTestTable),
          "xxxx"
        )(identity)
    
        val resultRows = Await.result(preparedQuery)

        resultRows.size must equal(0)
      }
    }

    "create an extension using CREATE EXTENSION" in {
      if(postgresAvailable) {
        val client = getClient
        val result = client.prepareAndExecute("CREATE EXTENSION hstore")
        Await.result(result)
      }
    }

    "support multi-statement DDL" in {
      if(postgresAvailable) {
        val client = getClient
        val result = client.prepareAndExecute(
          """
            |CREATE TABLE multi_one(id integer);
            |CREATE TABLE multi_two(id integer);
            |DROP TABLE multi_one;
            |DROP TABLE multi_two;
          """.stripMargin)
        Await.result(result)
      }
    }

    "throw a ServerError" when {
      "query has error" in {
        if (postgresAvailable) {
          val client = getClient
          cleanDb(client)

          val selectQuery = client.select(
            "SELECT * FROM %s WHERE unknown_column='hello_updated'".format(IntegrationSpec.pgTestTable)
          )(identity)

          a[ServerError] must be thrownBy {
            Await.result(selectQuery, queryTimeout)
          }
        }
      }

      "prepared query is missing parameters" in {
        if (postgresAvailable) {
          val client = getClient
          cleanDb(client)

          val preparedQuery = client.prepareAndQuery("SELECT * FROM %s WHERE str_field=$1 AND bool_field=$2"
            .format(IntegrationSpec.pgTestTable), "hello")(identity)

          a [ServerError] must be thrownBy {
            Await.result(
              preparedQuery,
              queryTimeout
            )
          }
        }
      }
    }
  }
}
