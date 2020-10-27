package com.twitter.finagle.postgresql

import com.twitter.finagle.PostgreSql
import com.twitter.finagle.ServiceFactory
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import org.specs2.execute.AsResult
import org.specs2.execute.Result

/**
 * Mixing to get a postgresl instance per spec.
 *
 * {{{
 *   class MySpec extends EmbeddedPgSqlEach {
 *
 *     "The thing" should {
 *       "work" in embeddedPsql() { (embedded, client) =>
 *       }
 *     }
 *   }
 *
 * }}}
 *
 * Both the embedded instance and the client can be configured by passing corresponding methods to
 * `embeddedPsql`.
 *
 * @see [[EmbeddedPgSqlSpec]] for a flavour for a whole spec
 */
trait EmbeddedPgSqlEach { _: PgSqlSpec =>

  final val TestDbUser = "postgres"
  final val TestDbPassword: Option[String] = None
  final val TestDbName = "postgres"

  def embeddedPsql[R: AsResult](
    configurePsql: EmbeddedPostgres.Builder => EmbeddedPostgres.Builder = identity,
    configureClient: PostgreSql.Client => PostgreSql.Client = identity,
  )(spec: (EmbeddedPostgres, ServiceFactory[Request, Response]) => R): Result = {

    val builder = EmbeddedPostgres.builder()
      .setCleanDataDirectory(true)
      .setErrorRedirector(ProcessBuilder.Redirect.INHERIT)
      .setOutputRedirector(ProcessBuilder.Redirect.INHERIT)

    val embeddedPgSql = configurePsql(builder).start
    val baseClient = PostgreSql.Client().withCredentials(TestDbUser, TestDbPassword).withDatabase(TestDbName)
    val client = configureClient(baseClient).newClient(s"localhost:${embeddedPgSql.getPort}")

    val result = AsResult(spec(embeddedPgSql, client))
    embeddedPgSql.close()
    result
  }
}
