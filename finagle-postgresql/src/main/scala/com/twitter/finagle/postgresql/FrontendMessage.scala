package com.twitter.finagle.postgresql

import com.twitter.finagle.postgresql.Types.Format
import com.twitter.finagle.postgresql.Types.Name
import com.twitter.finagle.postgresql.Types.Oid
import com.twitter.finagle.postgresql.Types.WireValue
import com.twitter.finagle.postgresql.transport.MessageEncoder
import com.twitter.io.Buf

sealed abstract class FrontendMessage {
  def toBuf: Buf
}

object FrontendMessage {

  final case class Version(major: Short, minor: Short)

  final object Version {
    val `3.0`: Version = Version(3, 0)
  }

  final case object SslRequest extends FrontendMessage {
    override def toBuf: Buf = MessageEncoder.toBuf(this)
  }

  sealed trait Replication // TODO

  final case class StartupMessage(
    version: Version = Version.`3.0`,
    user: String,
    database: Option[String] = None,
    replication: Option[Replication] = None,
    params: Map[String, String] = Map.empty)
      extends FrontendMessage {
    override def toBuf: Buf = MessageEncoder.toBuf(this)
  }

  final case class PasswordMessage(password: String) extends FrontendMessage {
    override def toBuf: Buf = MessageEncoder.toBuf(this)
  }

  final case object Sync extends FrontendMessage {
    override def toBuf: Buf = MessageEncoder.toBuf(this)
  }

  final case object Flush extends FrontendMessage {
    override def toBuf: Buf = MessageEncoder.toBuf(this)
  }

  final case class Query(value: String) extends FrontendMessage {
    override def toBuf: Buf = MessageEncoder.toBuf(this)
  }

  // Extended query
  final case class Parse(
    name: Name,
    statement: String,
    dataTypes: Seq[Oid],
  ) extends FrontendMessage {
    override def toBuf: Buf = MessageEncoder.toBuf(this)
  }

  final case class Bind(
    portal: Name,
    statement: Name,
    formats: Seq[Format],
    values: Seq[WireValue],
    resultFormats: Seq[Format],
  ) extends FrontendMessage {
    override def toBuf: Buf = MessageEncoder.toBuf(this)
  }

  sealed trait DescriptionTarget
  final object DescriptionTarget {
    final case object Portal extends DescriptionTarget
    final case object PreparedStatement extends DescriptionTarget
  }

  final case class Describe(
    name: Name,
    target: DescriptionTarget,
  ) extends FrontendMessage {
    override def toBuf: Buf = MessageEncoder.toBuf(this)
  }

  final case class Execute(
    portal: Name,
    maxRows: Int,
  ) extends FrontendMessage {
    override def toBuf: Buf = MessageEncoder.toBuf(this)
  }

  final case class Close(
    target: DescriptionTarget,
    name: Name,
  ) extends FrontendMessage {
    override def toBuf: Buf = MessageEncoder.toBuf(this)
  }

  // COPY
  final case class CopyData(bytes: Buf) extends FrontendMessage {
    override def toBuf: Buf = MessageEncoder.toBuf(this)
  }

  final case object CopyDone extends FrontendMessage {
    override def toBuf: Buf = MessageEncoder.toBuf(this)
  }

  final case class CopyFail(msg: String) extends FrontendMessage {
    override def toBuf: Buf = MessageEncoder.toBuf(this)
  }
}
