package com.twitter.finagle.redis.protocol

import com.twitter.io.Buf

case class AddSlots(slots: Seq[Int])
  extends Cluster("ADDSLOTS", slots.map(_.toString))

case class ClusterInfo() extends Cluster("INFO")

abstract class Cluster(sub: String, args: Seq[String] = Seq()) extends Command {
  def name: Buf = Command.CLUSTER

  override def body: Seq[Buf] = {
    Buf.Utf8(sub) +: args.map(Buf.Utf8.apply)
  }
}
