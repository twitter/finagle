package com.twitter.finagle.redis.protocol

import java.net.InetSocketAddress
import com.twitter.io.Buf

// data structure for slots
object ClusterNode {
  def apply(host: String, port: Int, id: Option[String] = None, flags: Seq[String] = Seq()) =
    new ClusterNode(new InetSocketAddress(host, port), id, flags)
}

case class ClusterNode(addr: InetSocketAddress, id: Option[String], flags: Seq[String]) {
  def isMyself: Boolean =
    flags.contains("myself")
}

case class Slots(start: Int, end: Int, master: ClusterNode, replicas: Seq[ClusterNode])

case class AddSlots(slots: Seq[Int]) extends Cluster("ADDSLOTS", slots.map(_.toString))

sealed trait SetSlotState

object SetSlotState {
  case object Migrating extends SetSlotState
  case object Importing extends SetSlotState
  case object Stable extends SetSlotState
  case object Node extends SetSlotState
}

case class SetSlot(command: SetSlotState, slot: Int, nodeId: Option[String])
    extends Cluster("SETSLOT", Seq(slot.toString, command.toString.toUpperCase) ++ nodeId)

case class ClusterInfo() extends Cluster("INFO")

case class ClusterSlots() extends Cluster("SLOTS")

case class Replicate(nodeId: String) extends Cluster("REPLICATE", Seq(nodeId))

case class Meet(addr: InetSocketAddress)
    extends Cluster("MEET", Seq(addr.getAddress.getHostAddress, addr.getPort.toString))

case class GetKeysInSlot(slot: Int, count: Int)
    extends Cluster("GETKEYSINSLOT", Seq(slot.toString, count.toString))

case class Nodes() extends Cluster("NODES")

abstract class Cluster(sub: String, args: Seq[String] = Seq()) extends Command {
  def name: Buf = Command.CLUSTER

  override def body: Seq[Buf] = {
    Buf.Utf8(sub) +: args.map(Buf.Utf8.apply)
  }
}
