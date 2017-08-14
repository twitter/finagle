package com.twitter.finagle.redis

import com.twitter.finagle.redis.protocol.StatusReply
import com.twitter.io.{Buf, ByteReader}
import com.twitter.util.Future
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util.{BufToString, ReplyFormat, StringToBuf}
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets.UTF_8

private[redis] trait ClusterCommands { self: BaseClient with BasicServerCommands =>

  private def toInfoMap(buf: Buf): Map[String, String] = {
    def readAll(acc: Map[String, String], reader: ByteReader): Map[String, String] = {
      reader.remainingUntil('\r') match {
        case -1 => acc
        case n =>
          val newAcc = reader.readString(n, UTF_8).split(":") match {
            case Array(key, value) => acc + (key -> value)
            case _ => acc
          }

          // jump past the '\r\n'
          reader.skip(2)
          readAll(newAcc, reader)
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

  private def toNodes(buf: Buf): Seq[ClusterNode] = {
    def readAll(acc: Seq[ClusterNode], reader: ByteReader): Seq[ClusterNode] = {
      reader.remainingUntil('\n') match {
        case -1 => acc
        case n =>
          val newAcc = reader.readString(n, UTF_8).split(" ").toList match {
            case nodeId :: hostPort :: flags :: master :: pingSent :: pongRecv :: epoch :: linkState :: slots =>
              val Array(host, port) = hostPort.split(":")
              acc :+ ClusterNode(host, port.toInt, id = Some(nodeId), flags = flags.split(",").toSeq)
            case _ => acc
          }

          // jump past the '\n'
          reader.skip(1)
          readAll(newAcc, reader)
      }
    }

    readAll(Seq(), ByteReader(buf))
  }

  def addSlots(slots: Seq[Int]): Future[Unit] =
    doRequest(AddSlots(slots)) {
      case StatusReply(_) => Future.Unit
    }

  def clusterInfo(): Future[Map[String, String]] =
    doRequest(ClusterInfo()) {
      case BulkReply(message) => Future.value(toInfoMap(message))
      case EmptyBulkReply => Future.value(Map())
    }

  def slots(): Future[Seq[Slots]] =
    doRequest(ClusterSlots()) {
      case MBulkReply(entries) =>
        Future.value(toSlots(entries))
      case EmptyMBulkReply => Future.Nil
    }

  def nodes(): Future[Seq[ClusterNode]] =
    doRequest(Nodes()) {
      case BulkReply(entries) => Future.value(toNodes(entries))
      case EmptyBulkReply => Future.Nil
    }

  def replicate(nodeId: String): Future[Unit] =
    doRequest(Replicate(nodeId)) {
      case StatusReply(_) => Future.Unit
    }

  def nodeId(): Future[Option[String]] =
    nodes().map { resp =>
      resp
        .flatMap(n => if(n.isMyself) n.id else None)
        .headOption
    }

  def infoMap(): Future[Map[String, String]] =
    info().map(_.map(toInfoMap(_)).getOrElse(Map()))

  def meet(addr: InetSocketAddress): Future[Unit] =
    doRequest(Meet(addr)) {
      case StatusReply(_) => Future.Unit
    }
}
