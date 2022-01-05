package com.twitter.finagle.mysql.harness

import com.twitter.concurrent.Once
import com.twitter.finagle.Mysql
import com.twitter.finagle.mysql.harness.EmbeddedDatabase.UserNameForInstance
import com.twitter.finagle.mysql.harness.EmbeddedDatabase.checkUserCache
import com.twitter.finagle.mysql.harness.EmbeddedDatabase.userCache
import com.twitter.finagle.mysql.harness.EmbeddedInstance.SetupTeardownTimeout
import com.twitter.finagle.mysql.harness.config.DatabaseConfig
import com.twitter.finagle.mysql.harness.config.User
import com.twitter.util.Await
import com.twitter.util.Future
import java.util.concurrent.ConcurrentHashMap
import java.util.function.BiFunction

object EmbeddedDatabase {
  // EmbeddedDatabase instances are cached by instance destination and database name.
  // This allows tests to share schemas if need be.
  //
  // Note, we don't evict entries from here and they live for the life of
  // the associated instances (which live for the life of the jvm).
  private val dbCache: ConcurrentHashMap[CacheKey, CacheValue] =
    new ConcurrentHashMap[CacheKey, CacheValue]()
  private val userCache: ConcurrentHashMap[UserNameForInstance, User] =
    new ConcurrentHashMap[UserNameForInstance, User]()
  private case class CacheKey(instanceDest: String, databaseName: String)
  private case class CacheValue(databaseConfig: DatabaseConfig, embeddedDatabase: EmbeddedDatabase)
  private case class UserNameForInstance(userName: String, instanceDest: String)

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
    val newCacheValue = CacheKey(instance.dest, config.databaseName)
    dbCache
      .compute(
        newCacheValue,
        checkDatabaseCache(config, instance)
      ).embeddedDatabase
  }

  /**
   * This function is meant to be called as part of the ConcurrentHashMap compute checks only.
   *
   * This function ensures the passed in user hasn't already been created on an instance with a
   * different password
   * @param newUser The user to check
   * @return the user to add to the cache
   */
  private def checkUserCache(newUser: User): BiFunction[UserNameForInstance, User, User] = {
    case (_, null) => newUser
    case (_, oldUser) if newUser == oldUser => newUser
    case (userNameForInstance, oldUser) =>
      throw new Exception(s"""
          |User ${userNameForInstance.userName} on ${userNameForInstance.instanceDest} has already been configured
          |Old configuration: $oldUser
          |New configuration: $newUser
          |""".stripMargin)
  }

  /**
   * This function is meant to be called as part of the ConcurrentHashMap compute checks only.
   *
   * This function ensures a database hasn't previously been configured with different users nor
   * different setup queries.
   * @param databaseConfig The configuration of the database being checked
   * @param instance The instance being checked against
   * @return the CacheValue to add to the database cache
   */
  private def checkDatabaseCache(
    databaseConfig: DatabaseConfig,
    instance: EmbeddedInstance
  ): BiFunction[CacheKey, CacheValue, CacheValue] = {
    case (_, null) =>
      databaseConfig.users.foreach(user => {
        val userNameForInstance = UserNameForInstance(user.name, instance.dest)
        userCache.compute(userNameForInstance, checkUserCache(user))
      })
      CacheValue(databaseConfig, new EmbeddedDatabase(databaseConfig, instance))
    case (_, oldCacheValue) if databaseConfig == oldCacheValue.databaseConfig =>
      oldCacheValue
    case (_, oldCacheValue) =>
      throw new Exception(s"""
        |Database ${oldCacheValue.databaseConfig.databaseName} has already been configured on ${instance.dest}
        |Old Configuration: ${oldCacheValue.databaseConfig}
        |New Configuration: $databaseConfig""".stripMargin)
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
      val userNameForInstance = UserNameForInstance(user.name, instance.dest)
      userCache.compute(
        userNameForInstance,
        checkUserCache(user)
      ) //Ensures user hasn't been previously created with a different password

      val createUserPrefix = s"CREATE USER IF NOT EXISTS'${user.name}'@'%%'"
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
