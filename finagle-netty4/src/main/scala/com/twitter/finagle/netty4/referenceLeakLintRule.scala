package com.twitter.finagle.netty4

import com.twitter.util.lint.{Category, Issue, Rule}

object referenceLeakLintRule {
  def rule(): Rule = Rule(Category.Runtime,
    "Reference leak detected",
    "A reference leak was detected, check your log files for leak tracing information."
  ) {
    val detected = trackReferenceLeaks.leaksDetected
    if (detected > 0) Seq(Issue(s"$detected leak(s) detected"))
    else Nil
  }
}
