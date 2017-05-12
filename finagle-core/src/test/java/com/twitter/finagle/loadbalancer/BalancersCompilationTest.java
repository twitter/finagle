package com.twitter.finagle.loadbalancer;

import scala.collection.immutable.Vector;
import scala.collection.immutable.VectorBuilder;

import org.junit.Test;

import com.twitter.finagle.NoBrokersAvailableException;
import com.twitter.finagle.ServiceFactory;
import com.twitter.finagle.stats.NullStatsReceiver;
import com.twitter.finagle.stats.StatsReceiver;
import com.twitter.finagle.util.Rngs;
import com.twitter.util.Activity;
import com.twitter.util.Duration;

public class BalancersCompilationTest {
  private final StatsReceiver sr = NullStatsReceiver.get();

  private final Vector<ServiceFactory<String, String>> vec =
    new VectorBuilder<ServiceFactory<String, String>>().result();

  private final NoBrokersAvailableException noBrokers =
    new NoBrokersAvailableException("test");

  @Test
  public void test() {
    Balancers
      .p2c(5, Rngs.threadLocal())
      .newBalancer(Activity.value(vec), sr, noBrokers);

    Balancers
      .p2cPeakEwma(Duration.fromSeconds(60), 5, Rngs.threadLocal())
      .newBalancer(Activity.value(vec), sr, noBrokers);

    Balancers
      .heap(new scala.util.Random())
      .newBalancer(Activity.value(vec), sr, noBrokers);

    Balancers
      .aperture(Duration.fromSeconds(5), 0.5, 2, 1, 5, Rngs.threadLocal(), false)
      .newBalancer(Activity.value(vec), sr, noBrokers);
  }
}
