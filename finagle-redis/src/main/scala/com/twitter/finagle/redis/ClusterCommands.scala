package com.twitter.finagle.redis

import com.twitter.finagle.redis.protocol.StatusReply
import com.twitter.io.{Buf, ByteReader}
import com.twitter.util.Future
import com.twitter.finagle.redis.protocol._
import java.nio.charset.StandardCharsets.UTF_8

private[redis] trait ClusterCommands { self: BaseClient =>

  private def extractInfo(buf: Buf): Map[String, String] = {
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

  def addSlots(slots: Seq[Int]): Future[Unit] =
    doRequest(AddSlots(slots)) {
      case StatusReply(_) => Future.Unit
    }

  def clusterInfo(): Future[Map[String, String]] =
    doRequest(ClusterInfo()) {
      case BulkReply(message) =>
        Future.value(extractInfo(message))
      case EmptyBulkReply => Future.value(Map())
    }
}
