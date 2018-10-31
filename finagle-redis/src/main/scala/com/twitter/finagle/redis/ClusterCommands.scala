package com.twitter.finagle.redis

import com.twitter.finagle.redis.protocol.StatusReply
import com.twitter.io.{Buf, ByteReader}
import com.twitter.util.Future
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util.{BufToString, ReplyFormat, StringToBuf}
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets.UTF_8
import scala.annotation.tailrec

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
      ClusterNode(host = toString(name), port = toInt(port), id = opts.headOption.map(toString))

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
          replicas = replicas.map(toNode)
        )

      case _ =>
        val msg = ReplyFormat.toString(entries)
        throw new ClusterDecodeError(s"Could not extract Cluster Slots from $msg")
    }

  private def toNodes(buf: Buf): Seq[ClusterNode] = {
    @tailrec
    def readAll(acc: Seq[ClusterNode], reader: ByteReader): Seq[ClusterNode] = {
      reader.remainingUntil('\n') match {
        case -1 => acc
        case n =>
          val newAcc = reader.readString(n, UTF_8).split(" ").toList match {
            case nodeId :: hostPort :: flags :: master :: pingSent :: pongRecv :: epoch :: linkState :: slots =>
              val Array(host, portClusterPort) = hostPort.split(":")
              // support for 4.0 which has a different format: <host>:<port>@<cluster bus port>
              val port = portClusterPort.split("@").head
              acc :+ ClusterNode(
                host,
                port.toInt,
                id = Some(nodeId),
                flags = flags.split(",").toSeq
              )
            case _ => acc
          }

          // jump past the '\n'
          reader.skip(1)
          readAll(newAcc, reader)
      }
    }

    readAll(Vector.empty, ByteReader(buf))
  }

  /**
   * Add a list of slots this server is responsible for
   * @param slots
   * @return unit
   */
  def addSlots(slots: Seq[Int]): Future[Unit] =
    doRequest(AddSlots(slots)) {
      case StatusReply(_) => Future.Unit
    }

  /**
   * Set the state for this slot
   * @param command The new slot state
   * @param slot
   * @param destinationId the target node id
   * @return unit
   */
  def setSlot(state: SetSlotState, slot: Int, destinationId: Option[String]): Future[Unit] =
    doRequest(SetSlot(state, slot, destinationId)) {
      case StatusReply(_) => Future.Unit
    }

  /**
   * Set this slot to be migrating
   * @param slot
   * @param destinationId the target node id
   * @return unit
   */
  def setSlotMigrating(slot: Int, destinationId: String): Future[Unit] =
    setSlot(SetSlotState.Migrating, slot, Some(destinationId))

  /**
   * Set this slot to be importing
   * @param slot
   * @param destinationId the source node id
   * @return unit
   */
  def setSlotImporting(slot: Int, sourceId: String): Future[Unit] =
    setSlot(SetSlotState.Importing, slot, Some(sourceId))

  /**
   * Set the slot to be associated with a node
   * @param slot
   * @param ownerId the source node id
   * @return unit
   */
  def setSlotNode(slot: Int, ownerId: String): Future[Unit] =
    setSlot(SetSlotState.Node, slot, Some(ownerId))

  /**
   * Returns the cluster info
   * @return A key, value map with the cluster info
   */
  def clusterInfo(): Future[Map[String, String]] =
    doRequest(ClusterInfo()) {
      case BulkReply(message) => Future.value(toInfoMap(message))
      case EmptyBulkReply => Future.value(Map())
    }

  /**
   * Returns a list of slots known by the server
   * @return A list of slots
   */
  def slots(): Future[Seq[Slots]] =
    doRequest(ClusterSlots()) {
      case MBulkReply(entries) =>
        Future.value(toSlots(entries))
      case EmptyMBulkReply => Future.Nil
    }

  /**
   * Returns a list of nodes known by the server
   * @return A list of cluster nodes
   */
  def nodes(): Future[Seq[ClusterNode]] =
    doRequest(Nodes()) {
      case BulkReply(entries) => Future.value(toNodes(entries))
      case EmptyBulkReply => Future.Nil
    }

  /**
   * Returns a list of keys stored by a slot on this server
   * @param slot the slot id
   * @param count number of keys to return, default is 10
   * @return A list of key bufs
   */
  def getKeysInSlot(slot: Int, count: Int = 10): Future[Seq[Buf]] =
    doRequest(GetKeysInSlot(slot, count)) {
      case MBulkReply(keys) => Future.value(ReplyFormat.toBuf(keys))
      case EmptyMBulkReply => Future.Nil
    }

  /**
   * Instructs the server to become a replica of the given node
   * @param nodeId
   * @return unit
   */
  def replicate(nodeId: String): Future[Unit] =
    doRequest(Replicate(nodeId)) {
      case StatusReply(_) => Future.Unit
    }

  /**
   * Return the cluster node id of the server
   * @return optional node id
   */
  def nodeId(): Future[Option[String]] =
    node().map(_.flatMap(_.id))

  /**
   * Return the cluster node for this server
   * @return optional node cluster node
   */
  def node(): Future[Option[ClusterNode]] =
    nodes().map(_.filter(_.isMyself).headOption)

  /**
   * Returns the server info
   * @return A key, value map with the server info
   */
  def infoMap(): Future[Map[String, String]] =
    info().map(_.map(toInfoMap(_)).getOrElse(Map()))

  /**
   * Instructs this cluster server to meet another server
   * @param addr InetSocketAddress of the server to meet
   * @return unit
   */
  def meet(addr: InetSocketAddress): Future[Unit] =
    doRequest(Meet(addr)) {
      case StatusReply(_) => Future.Unit
    }
}
