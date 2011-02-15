package com.twitter.finagle.tracing

object VMID {
  private[this] val id = management.ManagementFactory.getRuntimeMXBean.getName
  def apply() = id
}
