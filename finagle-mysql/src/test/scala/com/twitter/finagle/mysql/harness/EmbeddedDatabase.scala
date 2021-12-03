package com.twitter.finagle.mysql.harness

import com.twitter.concurrent.Once
import com.twitter.finagle.Mysql
import com.twitter.finagle.mysql.harness.EmbeddedInstance.SetupTeardownTimeout
import com.twitter.finagle.mysql.harness.config.DatabaseConfig
import com.twitter.util.Await
import com.twitter.util.Future
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Function

object EmbeddedDatabase {
  // EmbeddedDatabase instances are cached by instance destination and database name.
  // This allows tests to share schemas if need be.
  //
  // Note, we don't evict entries from here and they live for the life of
  // the associated instances (which live for the life of the jvm).
  private val dbCache: ConcurrentHashMap[CacheKey, EmbeddedDatabase] =
    new ConcurrentHashMap[CacheKey, EmbeddedDatabase]()

  private case class CacheKey(instanceDest: String, databaseName: String)

  /**
   * Get or create a new EmbeddedDatabase for the given instance destination and database name in
   * `config`. This database will be created using `instance`.
   *
   * @param instance The mysql instance that will contain this database.
   * @param config The configuration used to configure the database.
   *
   * @return an EmbeddedDatabase instance for the given config.
   */
  def getInstance(
    config: DatabaseConfig,
    instance: EmbeddedInstance
  ): EmbeddedDatabase = {
    dbCache.computeIfAbsent(
      CacheKey(instance.dest, config.databaseName),
      new Function[CacheKey, EmbeddedDatabase] {
        override def apply(t: CacheKey): EmbeddedDatabase = {
          new EmbeddedDatabase(config, instance)
        }
      }
    )
  }
}

/**
 * Manages the lifecycle of the database on the given `instance`.
 *
 * @param config Configuration for the database.
 * @param instance The instance that will host this database.
 */
final class EmbeddedDatabase(
  val config: DatabaseConfig,
  instance: EmbeddedInstance) {

  /**
   * Initializes the database via `instance`. This is guaranteed
   * to only happen once – even if called multiple times.
   */
  val init: () => Unit = Once {
    val createDbSql = Seq(s"CREATE DATABASE `${config.databaseName}`;")
    val createUsersSql = config.users.flatMap { user =>
      val createUserPrefix = s"CREATE USER '${user.name}'@'%%'"
      val createUserSuffix = user.password match {
        case Some(pwd) => s"IDENTIFIED BY '$pwd';"
        case None => ";"
      }
      val createUser = s"$createUserPrefix $createUserSuffix"
      val grantPerm =
        s"GRANT ${user.permission.value} ON `${config.databaseName}`.* TO '${user.name}'@'%%';"
      Seq(createUser, grantPerm)
    }
    val rootClient = instance.newRootUserClient()
    Await.result(
      Future
        .traverseSequentially(createDbSql ++ createUsersSql)(rootClient.query)
        .unit
        .before(rootClient.close(SetupTeardownTimeout)))

    //  run user defined setup queries
    if (config.setupQueries.nonEmpty) {
      val dbClient = Mysql.client.withDatabase(config.databaseName)
      val rootClient = instance.newRootUserClient(dbClient)
      Await.result(
        Future
          .traverseSequentially(config.setupQueries)(rootClient.query)
          .unit
          .before(rootClient.close(SetupTeardownTimeout)))
    }
  }
}
