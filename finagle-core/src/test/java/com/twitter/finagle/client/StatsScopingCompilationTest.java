package com.twitter.finagle.client;

import scala.collection.immutable.Map;

import com.twitter.finagle.stats.StatsReceiver;
import com.twitter.util.Function2;

/**
 * Just a compilation test for Java.
 */
class StatsScopingCompilationTest {
  public void testRetryFilter() {
    Function2<StatsReceiver, Map<String, Object>, StatsReceiver> datfunk =
      new Function2<StatsReceiver, Map<String, Object>, StatsReceiver>() {
        @Override
        public StatsReceiver apply(StatsReceiver stats, Map<String, Object> meta) {
          return stats;
        }
      };

    StatsScoping.Scoper scoper = new StatsScoping.Scoper(datfunk);
  }
}
