package com.twitter.finagle.postgresql

import com.whisk.docker.testkit.ContainerSpec
import com.whisk.docker.testkit.DockerReadyChecker
import com.spotify.docker.client.DefaultDockerClient
import com.spotify.docker.client.DockerClient
import com.whisk.docker.testkit._
import org.specs2.specification.BeforeAfterAll

import scala.concurrent.ExecutionContext

trait DockerTestKitForAll extends BeforeAfterAll {

  val dockerClient: DockerClient = DefaultDockerClient.fromEnv().build()

  val dockerExecutionContext: ExecutionContext = ExecutionContext.global

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

  val tag = sys.env.get("POSTGRES_VERSION")
    .map { v =>
      v.split('.').take(2).mkString(".")
    }
    .getOrElse("12.5")

  val PostgresAdvertisedPort = 5432

  val PostgresUser = "postgres"
  val PostgresPassword = "test-password"

  val baseEnv = Map(
    "POSTGRES_USER" -> PostgresUser,
    "POSTGRES_PASSWORD" -> PostgresPassword
  )

  def postgresContainerEnv: Map[String, String] = Map.empty

  def configure(spec: ContainerSpec): ContainerSpec = spec

  val postgresContainer = configure(
    ContainerSpec(s"postgres:$tag")
      .withExposedPorts(PostgresAdvertisedPort)
      .withEnv((baseEnv ++ postgresContainerEnv).toList.map { case (k, v) => s"$k=$v" }: _*)
      .withReadyChecker(
        DockerReadyChecker
          .Jdbc(
            driverClass = "org.postgresql.Driver",
            user = PostgresUser,
            password = Some(PostgresPassword)
          )
          .looped(15, 1.second)
      )
  ).toContainer

  override val managedContainers: ManagedContainers = postgresContainer.toManagedContainer
}
