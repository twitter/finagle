package com.twitter.finagle.redis

import com.twitter.finagle.redis.protocol.StatusReply
import com.twitter.io.{Buf, ByteReader}
import com.twitter.util.Future
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util.{BufToString, ReplyFormat}
import java.nio.charset.StandardCharsets.UTF_8

private[redis] trait ClusterCommands { self: BaseClient =>

  private def toInfoMap(buf: Buf): Map[String, String] = {
    def readAll(acc: Map[String, String], reader: ByteReader): Map[String, String] = {
      reader.remainingUntil('\r') match {
        case -1 => acc
        case n =>
          val Array(key, value) = reader.readString(n, UTF_8).split(":")
          // jump past the '\r\n'
          reader.skip(2)
          readAll(acc + (key -> value), reader)
      }
    }

    readAll(Map(), ByteReader(buf)) 
  }

  private def toInt(entry: Reply): Int = entry match {
    case IntegerReply(v) => v.toInt
    case _ =>
      val msg = ReplyFormat.toString(List(entry))
      throw new ClusterDecodeError(s"Could not extract an Integer from $msg")
  }

  private def toString(entry: Reply): String = entry match {
    case BulkReply(v) => BufToString(v)
    case _ =>
      val msg = ReplyFormat.toString(List(entry))
      throw new ClusterDecodeError(s"Could not extract a String from $msg")
  }


  private def toNode(entry: Reply): ClusterNode = entry match {
    // early versions only had name and port, new versions include an id
    case MBulkReply(name :: port :: opts) =>
      ClusterNode(
        host = toString(name),
        port = toInt(port),
        id = opts.headOption.map(toString))

    case _ =>
      val msg = ReplyFormat.toString(List(entry))
      throw new ClusterDecodeError(s"Could not extract a Cluster Node from $msg")
  }


  private def toSlots(entries: List[Reply]): Seq[Slots] =
    entries.map {
      case MBulkReply(start :: end :: master :: replicas) =>
        Slots(
          start = toInt(start),
          end = toInt(end),
          master = toNode(master),
          replicas = replicas.map(toNode))

      case _ =>
        val msg = ReplyFormat.toString(entries)
        throw new ClusterDecodeError(s"Could not extract Cluster Slots from $msg")
    }

  def addSlots(slots: Seq[Int]): Future[Unit] =
    doRequest(AddSlots(slots)) {
      case StatusReply(_) => Future.Unit
    }

  def clusterInfo(): Future[Map[String, String]] =
    doRequest(ClusterInfo()) {
      case BulkReply(message) =>
        Future.value(toInfoMap(message))
      case EmptyBulkReply => Future.value(Map())
    }

  def slots(): Future[Seq[Slots]] =
    doRequest(ClusterSlots()) {
      case MBulkReply(entries) =>
        Future.value(toSlots(entries))
      case EmptyMBulkReply => Future.Nil
    }
}
