package com.twitter.finagle.redis.protocol

import com.twitter.io.Buf

// data structure for slots
case class ClusterNode(host: String, port: Int, id: Option[String] = None)

case class Slots(start: Int, end: Int, master: ClusterNode, replicas: Seq[ClusterNode])

case class AddSlots(slots: Seq[Int])
  extends Cluster("ADDSLOTS", slots.map(_.toString))

case class ClusterInfo() extends Cluster("INFO")

case class ClusterSlots() extends Cluster("SLOTS")

abstract class Cluster(sub: String, args: Seq[String] = Seq()) extends Command {
  def name: Buf = Command.CLUSTER

  override def body: Seq[Buf] = {
    Buf.Utf8(sub) +: args.map(Buf.Utf8.apply)
  }
}
