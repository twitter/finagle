package com.twitter.finagle.postgresql

import org.specs2.mutable.Specification
import com.twitter.finagle.PostgreSql
import com.whisk.docker.testkit.ContainerSpec
import com.whisk.docker.testkit.DockerReadyChecker

import java.util.concurrent.ForkJoinPool

import com.spotify.docker.client.DefaultDockerClient
import com.spotify.docker.client.DockerClient
import com.whisk.docker.testkit._
import org.specs2.specification.BeforeAfterAll

import scala.concurrent.ExecutionContext

trait DockerTestKitForAll extends BeforeAfterAll {

  val dockerClient: DockerClient = DefaultDockerClient.fromEnv().build()

  val dockerExecutionContext: ExecutionContext = ExecutionContext.fromExecutor(new ForkJoinPool())

  val managedContainers: ManagedContainers

  val dockerTestTimeouts: DockerTestTimeouts = DockerTestTimeouts.Default

  implicit lazy val dockerExecutor: ContainerCommandExecutor =
    new ContainerCommandExecutor(dockerClient)

  lazy val containerManager = new DockerContainerManager(
    managedContainers,
    dockerExecutor,
    dockerTestTimeouts,
    dockerExecutionContext
  )

  def beforeAll(): Unit = containerManager.start()
  def afterAll(): Unit = containerManager.stop()
}

trait DockerPostgresService extends DockerTestKitForAll {
  import scala.concurrent.duration._

  val postgresAdvertisedPort = 5432

  val PostgresUser = "nph"
  val PostgresPassword = "suitup"

  val postgresContainer = ContainerSpec("postgres:9.6")
    .withExposedPorts(postgresAdvertisedPort)
    .withEnv(s"POSTGRES_USER=$PostgresUser", s"POSTGRES_PASSWORD=$PostgresPassword")
    .withReadyChecker(
      DockerReadyChecker
        .Jdbc(
          driverClass = "org.postgresql.Driver",
          user = PostgresUser,
          password = Some(PostgresPassword)
        )
        .looped(15, 1.second)
    )
    .toContainer

  override val managedContainers: ManagedContainers = postgresContainer.toManagedContainer
}

class DockerSpec extends Specification with PgSqlSpec with DockerPostgresService {

  "patate" should {
    "start a docker container" in {
      val sss = PostgreSql.Client()
        .withCredentials(PostgresUser, Some(PostgresPassword))
        .withDatabase("postgres")
        .newRichClient(s"localhost:${postgresContainer.mappedPort(5432)}")

      sss.select("SELECT 1;")(_.get[Int](0))
        .map { ints =>
          ints must haveSize(1)
          ints.head must_== 1
        }
    }
  }

}
