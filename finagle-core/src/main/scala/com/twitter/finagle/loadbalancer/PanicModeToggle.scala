package com.twitter.finagle.loadbalancer

import com.twitter.finagle.CoreToggles

private[loadbalancer] object PanicModeToggle {
  private val toggle = CoreToggles("com.twitter.finagle.loadbalancer.PanicMode")

  def apply(dest: String): Boolean = {
    toggle(dest.hashCode)
  }
}
