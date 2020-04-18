package com.twitter.finagle.postgresql

sealed trait FrontendMessage

object FrontendMessage {

  case class Version(major: Short, minor: Short)

  object Version {
    val `3.0` = Version(3, 0)
  }

  case object SslRequest extends FrontendMessage

  sealed trait Replication // TODO

  case class StartupMessage(
    version: Version = Version.`3.0`,
    user: String,
    database: Option[String] = None,
    replication: Option[Replication] = None,
    params: Map[String, String] = Map.empty
  ) extends FrontendMessage

  case class PasswordMessage(password: String) extends FrontendMessage

  case object Sync extends FrontendMessage

  case class Query(value: String) extends FrontendMessage

}
