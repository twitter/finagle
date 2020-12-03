package com.twitter.finagle.postgresql

import com.whisk.docker.testkit.ContainerSpec
import com.whisk.docker.testkit.DockerReadyChecker
import com.spotify.docker.client.DefaultDockerClient
import com.spotify.docker.client.DockerClient
import com.whisk.docker.testkit._
import org.specs2.execute.AsResult
import org.specs2.execute.Skipped
import org.specs2.specification.BeforeAfterAll
import org.specs2.main.ArgumentsShortcuts

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

sealed trait Backend {
  def image: String
  def advertisedPort: Int
  def user: String
  def password: Option[String] = None
  def cmd: Seq[String] = Nil
  def env: Map[String, String]
}
case object Postgres extends Backend {
  override def image: String = "postgres"
  override def advertisedPort: Int = 5432
  override def user: String = "postgres"
  override def password: Option[String] = Some("test-password")
  override def env: Map[String, String] =
    Map(
      "POSTGRES_USER" -> user,
      "POSTGRES_PASSWORD" -> password.get
    )
}
case object CockroachDb extends Backend {
  override def image: String = "cockroachdb/cockroach"
  override def advertisedPort: Int = 26257
  override def user: String = "root"
  override def env: Map[String, String] = Map.empty
  override def cmd: Seq[String] = Seq("start-single-node", "--insecure")
}

trait PostgresBackendService extends DockerTestKitForAll { _: ArgumentsShortcuts =>

  import scala.concurrent.duration._
  def testBackend: Backend = sys.env.get("TEST_BACKEND").map {
    case "postgres" => Postgres
    case "cockroachdb" => CockroachDb
  }.getOrElse(Postgres)

  def tag = sys.env.getOrElse("TEST_BACKEND_TAG", "13")

  /**
   * Only execute the spec for a specific backend
   *
   * {{{
   *   class MySpec extends PostgresBackendService {
   *     specificTo(Postgres)
   *
   *     "My tests" should {...}
   *   }
   * }}}
   */
  def specificTo(b: Backend) =
    skipAllIf(testBackend != b)

  /**
   * Used to make a single fragment backend-specific:
   *
   * {{{
   *   "my fragment" in backend(Postgres) {
   *     true
   *   }
   * }}}
   *
   * Will only execute the fragment when the backend is Postgres.
   */
  def backend[T: AsResult](b: Backend)(f: => T) =
    if (b == testBackend) AsResult(f)
    else {
      Skipped()
    }

  def env(backend: Backend): Map[String, String] = {
    val _ = backend
    Map.empty
  }

  def configure(backend: Backend, spec: ContainerSpec): ContainerSpec = {
    val _ = backend
    spec
  }

  val container = configure(
    testBackend,
    ContainerSpec(s"${testBackend.image}:$tag")
      .withExposedPorts(testBackend.advertisedPort)
      .withEnv((testBackend.env ++ env(testBackend)).toList.map { case (k, v) => s"$k=$v" }: _*)
      .withCommand(testBackend.cmd: _*)
      .withReadyChecker(
        DockerReadyChecker
          .Jdbc(
            driverClass = "org.postgresql.Driver",
            user = testBackend.user,
            password = testBackend.password
          )
          .looped(15, 1.second)
      )
  ).toContainer

  override val managedContainers: ManagedContainers = container.toManagedContainer

}
