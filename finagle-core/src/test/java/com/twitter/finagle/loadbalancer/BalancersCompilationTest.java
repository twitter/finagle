package com.twitter.finagle.loadbalancer;

import scala.collection.immutable.HashSet;
import scala.collection.immutable.Set;

import org.junit.Test;

import com.twitter.finagle.NoBrokersAvailableException;
import com.twitter.finagle.ServiceFactory;
import com.twitter.finagle.stats.NullStatsReceiver;
import com.twitter.finagle.stats.StatsReceiver;
import com.twitter.finagle.util.DefaultTimer;
import com.twitter.finagle.util.Rngs;
import com.twitter.util.Activity;
import com.twitter.util.Duration;

public class BalancersCompilationTest {
  private final StatsReceiver sr = NullStatsReceiver.get();

  private final Set<ServiceFactory<String, String>> set =
    new HashSet<ServiceFactory<String, String>>();

  private final NoBrokersAvailableException noBrokers =
    new NoBrokersAvailableException("test");

  @Test
  public void test() {
    Balancers
      .p2c(5, Rngs.threadLocal())
      .newBalancer(Activity.value(set), sr, noBrokers);

    Balancers
      .p2cPeakEwma(Duration.fromSeconds(60), 5, Rngs.threadLocal())
      .newBalancer(Activity.value(set), sr, noBrokers);

    Balancers
      .heap(new scala.util.Random())
      .newBalancer(Activity.value(set), sr, noBrokers);

    Balancers
      .aperture(Duration.fromSeconds(5), 0.5, 2, 1,
        DefaultTimer.twitter(), 5, Rngs.threadLocal())
      .newBalancer(Activity.value(set), sr, noBrokers);
  }
}
