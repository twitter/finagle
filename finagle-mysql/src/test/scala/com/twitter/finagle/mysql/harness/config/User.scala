package com.twitter.finagle.mysql.harness.config

/**
 * Defines a database user which can be configued on embedded instances.
 *
 * @param name The username for the user.
 * @param password The optional password for the user.
 * @param permission The permissions the user is given to the embedded db.
 */
case class User(
  name: String,
  password: Option[String],
  permission: User.Permission
)

object User {

  sealed trait Permission { def value: String }
  object Permission {
    case object All extends Permission { override val value: String = "ALL" }
    case object Select extends Permission { override val value: String = "SELECT" }
  }

  /**
   * The root user created for an embedded instance.
   */
  val Root = User("root", None, Permission.All)
}