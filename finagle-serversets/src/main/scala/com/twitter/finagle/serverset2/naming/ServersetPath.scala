package com.twitter.finagle.serverset2.naming

import com.twitter.finagle.util.parsers
import com.twitter.finagle.Path

/**
 * A fully parsed serverset path.
 *
 * @param zkHosts  the ZooKeeper ensemble
 * @param zkPath   path to the serverset
 * @param endpoint optional endpoint name (e.g. 'http')
 * @param shardId  optional shard ID (non-negative integer)
 */
private[twitter] case class ServersetPath(
  zkHosts: String,
  zkPath: Path,
  endpoint: Option[String],
  shardId: Option[Int])

/**
 * Parse serverset paths of the form:
 *
 *   /zkHosts/zkPath(:endpoint)?(#shardId)?
 */
private[twitter] object ServersetPath {
  def of(path: Path): Option[ServersetPath] = path match {
    case Path.Utf8(zkHosts, rest @ _*) =>
      rest.lastOption match {
        case Some(JobSyntax(name, endpoint, shardId)) =>
          val zkPath = Path.Utf8(rest.init: _*) ++ Path.Utf8(name)
          Some(ServersetPath(zkHosts, zkPath, endpoint, shardId))
        case Some(job) =>
          throw new IllegalArgumentException(s"invalid job syntax: $job")
        case None =>
          Some(ServersetPath(zkHosts, Path.empty, None, None))
      }
    case _ => None
  }

  private[this] object JobSyntax {
    sealed trait Token
    case class Elem(e: String) extends Token
    object Colon extends Token
    object NumberSign extends Token

    def lex(s: String): Seq[Token] = {
      s.foldLeft(List.empty[Token]) {
          case (ts, ':') => Colon :: ts
          case (ts, '#') => NumberSign :: ts
          case (Elem(s) :: ts, c) => Elem(s + c) :: ts
          case (ts, c) => Elem(c.toString) :: ts
        }
        .reverse
    }

    /**
     * Parse string as a job name, which may be optionally qualified
     * by endpoint and shard ID.
     *
     * Syntax: name(:job)?(#shard-id)?
     */
    def unapply(s: String): Option[(String, Option[String], Option[Int])] = lex(s) match {
      case Seq(Elem(name)) =>
        Some((name, None, None))
      case Seq(Elem(name), Colon, Elem(endpoint)) =>
        Some((name, Some(endpoint), None))
      case Seq(Elem(name), NumberSign, Elem(ShardId(shardId))) =>
        Some((name, None, Some(shardId)))
      case Seq(Elem(name), Colon, Elem(endpoint), NumberSign, Elem(ShardId(shardId))) =>
        Some((name, Some(endpoint), Some(shardId)))
      case _ =>
        None
    }
  }

  private[this] object ShardId {
    def unapply(s: String): Option[Int] =
      parsers.int.unapply(s).filter { _ >= 0 }
  }
}
