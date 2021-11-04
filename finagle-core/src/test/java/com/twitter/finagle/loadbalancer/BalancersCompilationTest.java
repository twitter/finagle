package com.twitter.finagle.loadbalancer;

import scala.Option;
import scala.collection.immutable.Vector;
import scala.collection.immutable.VectorBuilder;

import org.junit.Test;

import com.twitter.finagle.NoBrokersAvailableException;
import com.twitter.finagle.Stack;
import com.twitter.finagle.StackParams;
import com.twitter.finagle.stats.NullStatsReceiver;
import com.twitter.finagle.stats.StatsReceiver;
import com.twitter.finagle.util.Rngs;
import com.twitter.util.Activity;
import com.twitter.util.Duration;

public class BalancersCompilationTest {
  private final StatsReceiver sr = NullStatsReceiver.get();

  private final Vector<EndpointFactory<String, String>> vec =
    new VectorBuilder<EndpointFactory<String, String>>().result();

  private final NoBrokersAvailableException noBrokers =
    new NoBrokersAvailableException("test");

  @Test
  public void test() {
    Stack.Params emptyParams = StackParams.empty();

    Balancers
      .p2c(Rngs.threadLocal())
      .newBalancer(Activity.value(vec), noBrokers, emptyParams);

    Balancers
      .p2cPeakEwma(Duration.fromSeconds(60), Rngs.threadLocal())
      .newBalancer(Activity.value(vec), noBrokers, emptyParams);

    Balancers
      .heap(new scala.util.Random())
      .newBalancer(Activity.value(vec), noBrokers, emptyParams);

    Balancers
      .aperture(Duration.fromSeconds(5), 0.5, 2, 1,  Rngs.threadLocal(), Option.empty())
      .newBalancer(Activity.value(vec), noBrokers, emptyParams);
  }
}
