package com.twitter.finagle.postgres

import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.postgres.protocol.PgRequest
import com.twitter.finagle.postgres.protocol.PgResponse
import com.twitter.logging.Logger
import com.twitter.logging.config.FileHandlerConfig
import com.twitter.logging.config.LoggerConfig
import com.twitter.logging.config.Policy
import protocol.PgCodec
import com.twitter.finagle.postgres.protocol.Communication
import com.twitter.finagle.postgres.protocol.Query

object Main {
  private val logger = Logger(getClass.getName)

  def main(args: Array[String]) {
    import com.twitter.logging.config._

    val config = new LoggerConfig {
      node = ""
      level = Logger.DEBUG
      handlers = new ConsoleHandlerConfig {
      }
    }
    config()

    val client = Client("localhost:5432", "mkhadikov", None, "testing")

    val f = client.query("select * from pg_catalog.pg_stat_activity");

    logger.debug("Responded " + f.get())
    
    client.close()

  }

}
