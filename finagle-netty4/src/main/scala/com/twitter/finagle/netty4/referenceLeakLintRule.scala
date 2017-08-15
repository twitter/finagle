package com.twitter.finagle.netty4

import com.twitter.util.lint.{Category, Issue, Rule}
import java.util.concurrent.atomic.AtomicInteger

object referenceLeakLintRule {

  private[this] val leaksDetected: AtomicInteger = new AtomicInteger(0)

  def leakDetected(): Unit = leaksDetected.incrementAndGet()

  def rule(): Rule =
    Rule(
      Category.Runtime,
      "Reference leak detected",
      "A reference leak was detected, check your log files for leak tracing information."
    ) {
      val detected = leaksDetected.get()
      if (detected > 0) Seq(Issue(s"$detected leak(s) detected"))
      else Nil
    }
}
