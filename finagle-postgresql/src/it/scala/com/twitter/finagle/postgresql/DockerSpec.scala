package com.twitter.finagle.postgresql

import org.specs2.mutable.Specification
import java.sql.DriverManager

import com.twitter.finagle.PostgreSql
import com.whisk.docker.DockerCommandExecutor
import com.whisk.docker.DockerContainer
import com.whisk.docker.DockerContainerState
import com.whisk.docker.DockerKit
import com.whisk.docker.DockerReadyChecker
import com.whisk.docker.LogLineReceiver
import com.whisk.docker.impl.dockerjava.DockerKitDockerJava
import com.whisk.docker.specs2.DockerTestKit

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.util.Try

trait DockerPostgresService extends DockerKit {
  import scala.concurrent.duration._
  def PostgresAdvertisedPort = 5432
  def PostgresExposedPort = 44444
  val PostgresUser = "nph"
  val PostgresPassword = "suitup"

  val postgresContainer = DockerContainer("postgres:9.6")
    .withPorts((PostgresAdvertisedPort, Some(PostgresExposedPort)))
    .withEnv(s"POSTGRES_USER=$PostgresUser", s"POSTGRES_PASSWORD=$PostgresPassword")
    .withLogLineReceiver(LogLineReceiver(true, println))
    .withReadyChecker(
      new PostgresReadyChecker(PostgresUser, PostgresPassword, Some(PostgresExposedPort))
        .looped(15, 1.second)
    )

  abstract override def dockerContainers: List[DockerContainer] =
    postgresContainer :: super.dockerContainers
}

class PostgresReadyChecker(user: String, password: String, port: Option[Int] = None) extends DockerReadyChecker {

  override def apply(container: DockerContainerState)(implicit docker: DockerCommandExecutor, ec: ExecutionContext) =
    container
      .getPorts()
      .map(ports =>
        Try {
          Class.forName("org.postgresql.Driver")
          val url = s"jdbc:postgresql://${docker.host}:${port.getOrElse(ports.values.head)}/"
          Option(DriverManager.getConnection(url, user, password)).map(_.close).isDefined
        }.getOrElse(false)
      )
}

class DockerSpec
    extends Specification
    with PgSqlSpec
    with DockerTestKit
    with DockerPostgresService
    with DockerKitDockerJava {

  "patate" should {
    "start a docker container" in {
      Await.result(isContainerReady(postgresContainer), Duration.Inf)
      val sss = PostgreSql.Client()
        .withCredentials(PostgresUser, Some(PostgresPassword))
        .withDatabase("postgres")
        .newRichClient(s"localhost:$PostgresExposedPort")

      sss.select("SELECT 1;")(_.get[Int](0))
        .map { ints =>
          ints must haveSize(1)
          ints.head must_== 1
        }
    }
  }

}
