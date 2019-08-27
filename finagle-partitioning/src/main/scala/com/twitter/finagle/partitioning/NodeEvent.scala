package com.twitter.finagle.partitioning

private[finagle] sealed trait NodeEvent
private[finagle] sealed trait NodeHealth extends NodeEvent
private[finagle] case class NodeMarkedDead(key: KetamaClientKey) extends NodeHealth
private[finagle] case class NodeRevived(key: KetamaClientKey) extends NodeHealth
