package com.twitter.finagle.mysql.harness

import com.twitter.finagle.Mysql
import com.twitter.finagle.mysql.harness.EmbeddedMySqlInstance.SetupTeardownTimeout
import com.twitter.finagle.mysql.harness.config.{MySqlDatabaseConfig, MySqlUser}
import com.twitter.logging.Logger
import com.twitter.util.Try.OrThrow
import com.twitter.util.{Await, Future}
import java.util.concurrent.ConcurrentHashMap
import java.util.function.{BiFunction, Function}

object EmbeddedMySqlDatabase {

  // EmbeddedMySqlDatabase instances are cached by database name. This allows tests to share
  // schemas if need be.
  private val dbCache: ConcurrentHashMap[String, EmbeddedMySqlDatabase] =
    new ConcurrentHashMap[String, EmbeddedMySqlDatabase]()

  private val log: Logger = Logger.get()

  /**
   * Get or create a new MySqlDatabase for the given database name in [[MySqlDatabaseConfig]].
   * This database will be created in the given [[EmbeddedMySqlInstance]].
   * @param instance The mysql instance containing this database
   * @param config Config defining the database name, ReadWrite, and ReadOnly users
   * @param onDatabaseSetup This function will be called once the database has been setup for the
   *                        first time. This can be used to create the schema for the database.
   * @return an EmbeddedMySqlDatabase instance for the given database name
   */
  def getInstance(
    instance: EmbeddedMySqlInstance,
    config: MySqlDatabaseConfig,
    onDatabaseSetup: EmbeddedMySqlDatabase => Unit = _ => {}
  ): EmbeddedMySqlDatabase = {
    dbCache.computeIfAbsent(
      config.databaseName,
      new Function[String, EmbeddedMySqlDatabase] {
        override def apply(t: String): EmbeddedMySqlDatabase = {
          log.info(s"setting up database ${config.databaseName}")
          setupDatabase(instance, config)
          val database = new EmbeddedMySqlDatabase(config, instance)
          onDatabaseSetup(database)
          database
        }
      }
    )
  }

  /**
   * Drop the database and configured users from this instance
   * @param instance The instance containing this database
   * @param config The configuration of this database
   */
  def dropDatabase(
    instance: EmbeddedMySqlInstance,
    config: MySqlDatabaseConfig
  ): Unit = {
    dbCache.computeIfPresent(
      config.databaseName,
      new BiFunction[String, EmbeddedMySqlDatabase, EmbeddedMySqlDatabase] {
        override def apply(
          t: String,
          u: EmbeddedMySqlDatabase
        ): EmbeddedMySqlDatabase = {
          log.info(s"dropping database ${config.databaseName}")
          val sql = Seq(
            s"DROP USER '${config.users.rwUser.name}'@'%%';",
            s"DROP USER '${config.users.roUser.name}'@'%%';",
            s"DROP DATABASE `${config.databaseName}`;"
          )
          val rootClient = instance.createRootUserClient().newRichClient(instance.dest)

          Await.result(
            Future
              .traverseSequentially(sql)(rootClient.query)
              .unit
              .before(rootClient.close(SetupTeardownTimeout)))
          // Dropping the database also removes it from the dbCache, hence the null
          null
        }
      }
    )
  }

  private def setupDatabase(
    instance: EmbeddedMySqlInstance,
    config: MySqlDatabaseConfig
  ): Unit = {
    val sql = Seq(
      s"CREATE DATABASE `${config.databaseName}`;",
      s"CREATE USER '${config.users.roUser.name}'@'%%' IDENTIFIED BY '${config.users.roUser.password
        .orThrow(new RuntimeException("Read Only user must have a password")).get()}';",
      s"CREATE USER '${config.users.rwUser.name}'@'%%' IDENTIFIED BY '${config.users.rwUser.password
        .orThrow(new RuntimeException("Read Write user must have a password")).get()}';",
      s"GRANT ${config.users.roUser.userType.value} ON `${config.databaseName}`.* TO '${config.users.roUser.name}'@'%%';",
      s"GRANT ${config.users.rwUser.userType.value} ON `${config.databaseName}`.* TO '${config.users.rwUser.name}'@'%%';"
    )
    val rootClient = instance.createRootUserClient().newRichClient(instance.dest)

    Await.result(
      Future
        .traverseSequentially(sql)(rootClient.query)
        .unit
        .before(rootClient.close(SetupTeardownTimeout)))
  }
}

/**
 * A MySql database
 * @param config Configuration for the database
 * @param mySqlInstance The instance that will host this database
 */
final class EmbeddedMySqlDatabase(
  config: MySqlDatabaseConfig,
  mySqlInstance: EmbeddedMySqlInstance) {

  /**
   * Create a client for the ReadOnly user defined for this database
   * @param baseClient base configured client to use
   * @return a client configured for the ReadOnly user
   */
  def createROClient(baseClient: Mysql.Client = Mysql.client): Mysql.Client = {
    createClient(config.users.roUser, baseClient)
  }

  /**
   * Create a client for the ReadWrite user defined for this database
   * @param baseClient base configured client to use
   * @return a client configured for the ReadWrite user
   */
  def createRWClient(baseClient: Mysql.Client = Mysql.client): Mysql.Client = {
    createClient(config.users.rwUser, baseClient)
  }

  /**
   * Create a client.
   * @param mysqlUser The MySqlUser to create this client for
   * @param baseClient Base configured client
   * @return
   */
  private def createClient(
    mysqlUser: MySqlUser,
    baseClient: Mysql.Client = Mysql.client
  ): Mysql.Client = {
    baseClient
      .withDatabase(config.databaseName)
      .withCredentials(mysqlUser.name, mysqlUser.password.orNull)
  }
}
