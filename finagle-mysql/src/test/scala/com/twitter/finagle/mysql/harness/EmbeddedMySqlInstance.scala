package com.twitter.finagle.mysql.harness

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Mysql
import com.twitter.finagle.mysql.Client
import com.twitter.finagle.mysql.harness.EmbeddedMySqlInstance.{RootUser, SetupTeardownTimeout}
import com.twitter.finagle.mysql.harness.config.{MySqlInstanceConfig, MySqlUser, MySqlUserType}
import com.twitter.logging.Logger
import com.twitter.util.{Await, Duration, Future, Return, Throw, Try}
import java.io.IOException
import java.net.{InetAddress, ServerSocket}
import java.nio.file.{Files, Path}
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Function
import scala.reflect.io.Directory

/**
 * Ensures only one embedded mysql instance is created for any database across all tests run.
 */
object EmbeddedMySqlInstance {
  val SetupTeardownTimeout: Duration = 5.seconds
  val RootUser: MySqlUser = new MySqlUser {
    override val name: String = "root"
    override val password: Option[String] = None
    override val userType: MySqlUserType = MySqlUserType.RW
  }

  val log: Logger = Logger.get()

  // EmbeddedMySqlInstance is cached by MySqlInstanceConfig in order to ensure 1 instance for all
  // tests. This will prevent multiple instances of the same mysql version to be spawned affecting
  // performance and memory.
  private val instanceCache: ConcurrentHashMap[MySqlInstanceConfig, EmbeddedMySqlInstance] =
    new ConcurrentHashMap[MySqlInstanceConfig, EmbeddedMySqlInstance]()

  private def configToInstance(
    config: MySqlInstanceConfig
  ): Function[MySqlInstanceConfig, EmbeddedMySqlInstance] =
    new Function[MySqlInstanceConfig, EmbeddedMySqlInstance] {
      override def apply(v1: MySqlInstanceConfig): EmbeddedMySqlInstance = {
        MySqlExecutables
          .fromPath(config.extractedMySqlPath)
          .map { executables =>
            log.debug("New instance is being set up")
            val dataDirectory: Path = createDataDirectory(config)
            val port = openPort()
            val dest = s"${InetAddress.getLoopbackAddress.getHostAddress}:$port"
            val serverParameters: Seq[String] = getServerParameters(config, dataDirectory, port)

            initializeDb(dataDirectory, executables)

            val instance = new EmbeddedMySqlInstance(executables, serverParameters, port, dest)
            instance.startInstance()

            sys addShutdownHook {
              instance.stopInstance()
              new Directory(dataDirectory.toFile).deleteRecursively()
            }
            instance
          }.orNull
      }
    }

  private def getServerParameters(
    config: MySqlInstanceConfig,
    dataDirectory: Path,
    port: Int
  ): Seq[String] = {
    val derivedServerParameters: Seq[String] = Seq(
      s"--datadir=$dataDirectory",
      s"--port=$port",
      s"--socket=${Files.createTempFile(null, ".sock")}"
    )
    config.startServerParameters ++ derivedServerParameters
  }

  /**
   * Get or create a new MySqlInstance for the given config. This instance has a port, server
   * parameters, and paths to mysqld and mysqladmin.
   * @param config Config containing the mysql version, path to the extracted distribution, and the
   *               server start parameters
   * @return The option will be empty if the executables are not found in the extraction directory
   */
  def getInstance(
    config: MySqlInstanceConfig
  ): Option[EmbeddedMySqlInstance] = {
    Option(instanceCache.computeIfAbsent(config, configToInstance(config)))
  }

  private def openPort(): Int = {
    val socket: ServerSocket = new ServerSocket(0)
    (Try {
      socket.setReuseAddress(true)
      socket.getLocalPort
    } ensure {
      if (socket != null) {
        try {
          socket.close()
        } catch {
          case _: IOException => ()
        }
      }
    }).getOrElse(throw new RuntimeException("Failed to get ephemeral port for embedded mysql"))
  }

  private def createDataDirectory(config: MySqlInstanceConfig): Path = {
    val dataDirectory: Path =
      config.extractedMySqlPath.resolve(s"data/${UUID.randomUUID()}")
    new Directory(dataDirectory.toFile).deleteRecursively()
    dataDirectory.toFile.mkdirs()
    dataDirectory
  }

  /**
   * Initialize the MySql --datadir
   * @param dataDirectory The location of --datadir
   * @param executables The location of the MySql executables
   */
  private def initializeDb(dataDirectory: Path, executables: MySqlExecutables): Unit = {
    log.info(s"Initializing $dataDirectory")
    executables.initDatabase(
      Seq(
        s"--datadir=$dataDirectory",
        "--initialize-insecure",
        // mysql_native_password is used for the default authentication plugin as a convenience when
        // comparing tests run against mysql 5 and 8.
        // this will eventually be abandoned for more secure authentication plugins
        "--default-authentication-plugin=mysql_native_password"
      ))
  }
}

/**
 * A MySql instance
 * @param mySqlExecutables locations of mysqld and mysqladmin
 * @param serverParameters the server parameters to start this instance with
 * @param port the port for start this instance on
 */
final class EmbeddedMySqlInstance(
  mySqlExecutables: MySqlExecutables,
  serverParameters: Seq[String],
  val port: Int,
  val dest: String) {
  private val running = new AtomicBoolean(false)
  private val log: Logger = Logger.get()

  /**
   * Start the MySql instance using the [[serverParameters]] on the specified [[port]]
   */
  def startInstance(): Unit = {
    if (!running.getAndSet(true)) {
      log.info(s"Starting mysql instance on port $port")
      mySqlExecutables.startDatabase(serverParameters)
      waitForDatabase()
    }
  }

  /**
   * Stop the MySql instance on the specified [[port]]
   */
  def stopInstance(): Unit = {
    if (running.getAndSet(false)) {
      log.info(s"Stopping mysql instance on port $port")
      mySqlExecutables.stopDatabase(port)
      log.info(s"mysql instance on port $port stopped cleanly")
    }
  }

  /**
   * Ping the database until it is up
   */
  private def waitForDatabase(): Unit = {
    val rootClient = createRootUserClient().newRichClient(dest)
    val wait = waitForPing(rootClient)
      .before(rootClient.close(SetupTeardownTimeout))
    Await.result(wait, SetupTeardownTimeout)
  }

  private def waitForPing(client: Client): Future[Unit] = {
    client.ping().transform {
      case Throw(_) =>
        waitForPing(client)
      case Return(_) =>
        log.info(s"mysql instance on port $port is available")
        Future.Done
    }
  }

  /**
   * Create a client for the root user
   * @param baseClient Base configured client
   * @return A client configured for the root user
   */
  def createRootUserClient(baseClient: Mysql.Client = Mysql.client): Mysql.Client = {
    baseClient
      .withCredentials(RootUser.name, RootUser.password.orNull)
  }
}
