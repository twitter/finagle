package com.twitter.finagle.stats;

import com.twitter.finagle.stats.StatsReceiver;

/**
 * Just a compilation test for Java.
 */

class JavaStatsReceiver {
  static {
    StatsReceiver statsReceiver = new NullStatsReceiver();
    Counter counter = statsReceiver.counter0("counter_name");
    Stat stat = statsReceiver.stat0("gauge");
  };
}
