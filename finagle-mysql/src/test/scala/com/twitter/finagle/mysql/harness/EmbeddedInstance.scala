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
import java.io.File
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
import scala.sys.process.Process

/**
 * Manages lifecycle of the embedded mysql instance.
 */
object EmbeddedInstance {
  val SocketParamName = "--socket="
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
  ): Seq[String] = {
    val derivedServerParameters: Seq[String] = Seq(
      s"--basedir=$baseDir",
      s"--datadir=$dataDirectory",
      s"--port=$port"
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

            // Will be empty if the socketParam is already present in order to ensure no duplicates
            val socketParamToAdd: Seq[String] =
              config.startServerParameters.find(_.toLowerCase.startsWith(SocketParamName)) match {
                case Some(param) =>
                  val socketFileName = param.substring(SocketParamName.length)
                  // Files.createTempFile throws if the file already exists. If the user defines
                  // the socket file in that manner, we want to ensure the file is deleted on exit
                  // which is why the file is created with createNewFile which does not throw
                  val socketFile = new File(socketFileName)
                  socketFile.createNewFile()
                  socketFile.deleteOnExit()
                  Seq.empty[String]
                case _ =>
                  val socketFile: Path =
                    Files.createTempFile(config.extractedMySqlPath, null, ".sock")
                  socketFile.toFile.deleteOnExit()
                  Seq(s"--socket=$socketFile")
              }

            val serverParameters: Seq[String] =
              getServerParameters(
                config,
                executables.getBaseDir,
                dataDirectory,
                port) ++ socketParamToAdd

            initializeDataDir(dataDirectory, executables.getBaseDir, executables)

            val instance = new EmbeddedInstance(executables, serverParameters, port, dest)
            instance.startInstance()

            sys.addShutdownHook {
              instanceCache.values().forEach { instance =>
                Try(instance.stopInstance()).handle {
                  case e =>
                    log.error(s"instance shutdown error: ${e.getMessage}")
                    instance.destroy()
                }
              }
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
  private var process: Process = _

  private def destroy(): Unit = {
    if (process.isAlive()) {
      log.info(s"mysql process still alive, destroying process")
      Try(process.destroy()).handle {
        case e => log.error(s"process destroy returned: ${e.getMessage}")
      }
    }
  }

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
      process = mySqlExecutables.startDatabase(serverParameters)
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
    // This client is configured without failure accrual nor fail fast in order to quickly
    // react to the Mysql server being available. Without these options, there are rare
    // scenarios where the client would be marked as dead even though the server comes up
    // which would cause test suites to fail.
    val rootClient = newRootUserClient(
      Mysql.client.withSessionQualifier.noFailureAccrual.withSessionQualifier.noFailFast)
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
