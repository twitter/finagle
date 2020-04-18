package com.twitter.finagle.postgresql

import com.twitter.finagle.postgresql.Types.Format
import com.twitter.finagle.postgresql.Types.Name
import com.twitter.finagle.postgresql.Types.Oid
import com.twitter.finagle.postgresql.Types.WireValue

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

  // Extended query
  case class Parse(
    name: Name,
    statement: String,
    dataTypes: Seq[Oid],
  ) extends FrontendMessage

  case class Bind(
    portal: Name,
    statement: Name,
    formats: Seq[Format],
    values: Seq[WireValue],
    resultFormats: Seq[Format],
  ) extends FrontendMessage

  sealed trait DescriptionTarget
  object DescriptionTarget {
    case object Portal extends DescriptionTarget
    case object PreparedStatement extends DescriptionTarget
  }

  case class Describe(
    name: Name,
    target: DescriptionTarget,
  ) extends FrontendMessage

  case class Execute(
    portal: Name,
    maxRows: Int,
  ) extends FrontendMessage

}
