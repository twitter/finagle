package com.twitter.finagle.client;

import scala.collection.immutable.Map;

import org.junit.Test;

import com.twitter.finagle.stats.StatsReceiver;
import com.twitter.util.Function2;

/**
 * Just a compilation test for Java.
 */
public class StatsScopingCompilationTest {
  @Test
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
