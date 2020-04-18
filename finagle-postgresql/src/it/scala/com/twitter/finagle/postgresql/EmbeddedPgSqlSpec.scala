package com.twitter.finagle.postgresql

import com.twitter.finagle.PostgreSql
import com.twitter.finagle.Service
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import org.specs2.specification.BeforeAfterAll

trait EmbeddedPgSqlSpec extends BeforeAfterAll {

  var embeddedPgSql: Option[EmbeddedPostgres] = None

  final val TestDbUser = "postgres"
  final val TestDbPassword: Option[String] = None
  final val TestDbName = "postgres"

  def configure(b: EmbeddedPostgres.Builder): EmbeddedPostgres.Builder = b

  def prep(e: EmbeddedPostgres): EmbeddedPostgres = e

  def client(cfg: PostgreSql.Client => PostgreSql.Client): Service[Request, Response] = embeddedPgSql match {
    case None => sys.error("getClient invoked outside of test fragment")
    case Some(pgsql) =>
      cfg(
        PostgreSql.Client()
          .withCredentials(TestDbUser, TestDbPassword)
          .withDatabase(TestDbName)
      ).newService(s"localhost:${pgsql.getPort}")
  }

  def client: Service[Request, Response] = client(identity)

  override def beforeAll(): Unit = {
    val builder =
      EmbeddedPostgres.builder()
        .setCleanDataDirectory(true)
        .setErrorRedirector(ProcessBuilder.Redirect.INHERIT)
        .setOutputRedirector(ProcessBuilder.Redirect.INHERIT)

    embeddedPgSql = Some(prep(configure(builder).start()))
  }

  override def afterAll(): Unit = embeddedPgSql.foreach(_.close())

}
