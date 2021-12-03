package com.twitter.finagle.mysql.harness

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Mysql
import com.twitter.finagle.mysql.Client
import com.twitter.finagle.mysql.harness.config.InstanceConfig
import com.twitter.finagle.mysql.harness.config.User
import com.twitter.logging.Logger
import com.twitter.util.Await
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.Return
import com.twitter.util.Throw
import com.twitter.util.Try
import java.io.IOException
import java.net.InetAddress
import java.net.ServerSocket
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Function
import scala.reflect.io.Directory

/**
 * Manages lifecycle of the embedded mysql instance.
 */
object EmbeddedInstance {
  val SetupTeardownTimeout: Duration = 5.seconds

  val log: Logger = Logger.get()

  // EmbeddedInstance is cached by InstanceConfig in order to ensure 1 instance for all
  // tests. This will prevent multiple instances of the same mysql version to be spawned affecting
  // performance and memory.
  private val instanceCache: ConcurrentHashMap[InstanceConfig, EmbeddedInstance] =
    new ConcurrentHashMap[InstanceConfig, EmbeddedInstance]()

  private def getServerParameters(
    config: InstanceConfig,
    baseDir: Path,
    dataDirectory: Path,
    port: Int,
    socketFile: Path
  ): Seq[String] = {
    val derivedServerParameters: Seq[String] = Seq(
      s"--basedir=$baseDir",
      s"--datadir=$dataDirectory",
      s"--port=$port",
      s"--socket=$socketFile"
    )
    config.startServerParameters ++ derivedServerParameters
  }

  private def configToInstance(
    config: InstanceConfig
  ): Function[InstanceConfig, EmbeddedInstance] =
    new Function[InstanceConfig, EmbeddedInstance] {
      override def apply(v1: InstanceConfig): EmbeddedInstance = {
        MySqlExecutables
          .fromPath(config.extractedMySqlPath)
          .map { executables =>
            log.debug("New instance is being set up")
            val dataDirectory: Path = createDataDirectory(config)
            val port = openPort()
            val dest = s"${InetAddress.getLoopbackAddress.getHostAddress}:$port"
            val socketFile = Files.createTempFile(config.extractedMySqlPath, null, ".sock")
            val serverParameters: Seq[String] =
              getServerParameters(config, executables.getBaseDir, dataDirectory, port, socketFile)

            initializeDataDir(dataDirectory, executables.getBaseDir, executables)

            val instance = new EmbeddedInstance(executables, serverParameters, port, dest)
            instance.startInstance()

            sys.addShutdownHook {
              instance.stopInstance()
              socketFile.toFile.delete()
              new Directory(dataDirectory.toFile).deleteRecursively()
            }
            instance
          }.orNull
      }
    }

  /**
   * Get or create a new MySqlInstance for the given config. This instance has a port, server
   * parameters, and paths to mysqld and mysqladmin.
   * @param config Config containing the mysql version, path to the extracted distribution, and the
   *               server start parameters
   * @return The option will be empty if the executables are not found in the extraction directory
   */
  def getInstance(
    config: InstanceConfig
  ): Option[EmbeddedInstance] =
    Option(instanceCache.computeIfAbsent(config, configToInstance(config)))

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

  private def createDataDirectory(config: InstanceConfig): Path = {
    val dataDirectory: Path =
      config.extractedMySqlPath.resolve(s"data/${UUID.randomUUID()}")
    new Directory(dataDirectory.toFile).deleteRecursively()
    dataDirectory.toFile.mkdirs()
    dataDirectory
  }

  /**
   * Initialize the MySql --datadir.
   *
   * @param dataDirectory The location of --datadir
   * @param baseDirectory The location of --basedir
   * @param executables The location of the MySql executables
   */
  private def initializeDataDir(
    dataDirectory: Path,
    baseDirectory: Path,
    executables: MySqlExecutables
  ): Unit = {
    log.info(s"Initializing $dataDirectory")
    executables.initDatabase(
      Seq(
        s"--basedir=$baseDirectory",
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
 * A MySql instance.
 *
 * @param mySqlExecutables locations of mysqld and mysqladmin
 * @param serverParameters the server parameters to start this instance with
 * @param port the port for start this instance on
 */
final class EmbeddedInstance(
  mySqlExecutables: MySqlExecutables,
  serverParameters: Seq[String],
  val port: Int,
  val dest: String) {
  import EmbeddedInstance.SetupTeardownTimeout

  private val running = new AtomicBoolean(false)
  private val log: Logger = Logger.get()

  /**
   * Creates a new mysql.Client connected to the instance via the root account.
   */
  def newRootUserClient(baseClient: Mysql.Client = Mysql.client): Client =
    baseClient
      .withCredentials(User.Root.name, User.Root.password.orNull)
      .newRichClient(dest)

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
    val rootClient = newRootUserClient()
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
}
