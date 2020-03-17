package com.twitter.finagle.partitioning

/**
 * Provides hashing key information of the [[com.twitter.hashing.HashNode]]s
 * on consistent-hashing hash ring.
 */
sealed trait HashNodeKey {

  /** HashNode hashing key identifier, either "host:port" or custom format. */
  def identifier: String
}

/**
 * Constructs HashNodeKey based on the PartitionNode address. It can be either
 * a "host:port" format or custom format.
 */
object HashNodeKey {
  private[finagle] case class HostPortBasedHashNodeKey(host: String, port: Int, weight: Int)
      extends HashNodeKey {
    val identifier: String = if (port == 11211) host else host + ":" + port
  }
  private[finagle] case class CustomHashNodeKey(identifier: String) extends HashNodeKey

  def apply(host: String, port: Int, weight: Int): HashNodeKey =
    HostPortBasedHashNodeKey(host, port, weight)

  def apply(id: String): CustomHashNodeKey = CustomHashNodeKey(id)

  def fromPartitionNode(node: PartitionNode): HashNodeKey = node.key match {
    case Some(id) => HashNodeKey(id)
    case None => HashNodeKey(node.host, node.port, node.weight)
  }
}
