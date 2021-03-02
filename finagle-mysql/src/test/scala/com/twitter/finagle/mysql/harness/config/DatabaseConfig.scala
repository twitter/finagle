package com.twitter.finagle.mysql.harness.config

/**
 * The configuration used when creating an embedded db.
 *
 * @param databaseName The name of the database to create on the embedded instance.
 * @param users The users that will receive access to this db.
 * @param setupQueries The queries to run once this db is setup. The queries are run
 *                     using the read-write user for the db.
 */
final case class DatabaseConfig(
  databaseName: String,
  users: Seq[User],
  setupQueries: Seq[String]
)
