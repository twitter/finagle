package com.twitter.finagle.stats

import org.scalatest.funsuite.AnyFunSuite

class SummarizingStatsReceiverTest extends AnyFunSuite {
  test("SummarizingStatsReceiver doesn't fail on empty/low stats") {
    val receiver = new SummarizingStatsReceiver
    assert(receiver.summary() == "# counters\n\n# gauges\n\n# stats\n")

    val stats = receiver.stat("toto")
    stats.add(1)
    val expected =
      """# counters
      |
      |# gauges
      |
      |# stats
      |toto                           n=1 min=1.0 med=1.0 p90=1.0 p95=1.0 p99=1.0 p999=1.0 p9999=1.0 max=1.0""".stripMargin
    assert(receiver.summary() == expected)

    (2 to 10) foreach { stats.add(_) }
    val expected2 =
      """# counters
      |
      |# gauges
      |
      |# stats
      |toto                           n=10 min=1.0 med=6.0 p90=10.0 p95=10.0 p99=10.0 p999=10.0 p9999=10.0 max=10.0""".stripMargin
    assert(receiver.summary() == expected2)
  }
}
