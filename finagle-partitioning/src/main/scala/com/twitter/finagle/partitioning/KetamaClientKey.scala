package com.twitter.finagle.partitioning

abstract class KetamaClientKey {
  def identifier: String
}

object KetamaClientKey {
  private[finagle] case class HostPortBasedKey(host: String, port: Int, weight: Int)
      extends KetamaClientKey {
    val identifier: String = if (port == 11211) host else host + ":" + port
  }
  private[finagle] case class CustomKey(identifier: String) extends KetamaClientKey

  def apply(host: String, port: Int, weight: Int): KetamaClientKey =
    HostPortBasedKey(host, port, weight)

  def apply(id: String): CustomKey = CustomKey(id)

  def fromCacheNode(node: PartitionNode): KetamaClientKey = node.key match {
    case Some(id) => KetamaClientKey(id)
    case None => KetamaClientKey(node.host, node.port, node.weight)
  }
}
