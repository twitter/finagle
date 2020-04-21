package com.twitter.finagle.postgresql

import com.twitter.finagle.PostgreSql
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import org.specs2.specification.BeforeAfterAll

trait EmbeddedPgSqlSpec extends BeforeAfterAll { _: PgSqlSpec =>

  var embeddedPgSql: Option[EmbeddedPostgres] = None

  final val TestDbUser = "postgres"
  final val TestDbPassword: Option[String] = None
  final val TestDbName = "postgres"

  def withPgSql[T](f: EmbeddedPostgres => T): T = embeddedPgSql match {
    case None => sys.error("getClient invoked outside of test fragment")
    case Some(pgsql) => f(pgsql)
  }

  def configure(b: EmbeddedPostgres.Builder): EmbeddedPostgres.Builder = b

  def prep(e: EmbeddedPostgres): EmbeddedPostgres = e

  def randomTableName = util.Random.alphanumeric.filter(c => c >= 'a' && c <= 'z').take(10).mkString
  def withTmpTable[T](f: String => T) = withPgSql { pgsql =>
    using(pgsql.getDatabase(TestDbUser, TestDbName).getConnection()) { conn =>
      using(conn.createStatement()) { stmt =>
        val tableName = randomTableName
        stmt.execute(s"CREATE TABLE $tableName(int_col int)") // TODO: allow specifying the table spec
        f(tableName)
      }
    }
  }

  def newClient(cfg: PostgreSql.Client => PostgreSql.Client): ServiceFactory[Request, Response] = withPgSql { pgsql =>
    cfg(
      PostgreSql.Client()
        .withCredentials(TestDbUser, TestDbPassword)
        .withDatabase(TestDbName)
    ).newClient(s"localhost:${pgsql.getPort}")
  }

  def client(cfg: PostgreSql.Client => PostgreSql.Client): Service[Request, Response] =
    newClient(cfg).toService

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
