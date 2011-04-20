package com.twitter.finagle.tracing

private[tracing] object VMID {
  private[this] val id = management.ManagementFactory.getRuntimeMXBean.getName
  def apply() = id
}
